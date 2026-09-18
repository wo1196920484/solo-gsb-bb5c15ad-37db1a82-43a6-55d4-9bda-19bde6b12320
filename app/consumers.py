"""Consumer cursors and claim/ack/nack leases for event consumption.

Each ``(stream_id, consumer_id)`` owns a cursor (``cursor_seq``): the sequence
of the next event the consumer needs to receive. A ``claim`` hands out up to
``max_events`` contiguous *applied* events starting at the cursor and records a
time-limited lease on that exact batch. An ``ack`` carrying the lease token
atomically advances the cursor past the batch, but only while the lease is
alive. After expiry the same batch may be claimed again (at-least-once
delivery).

A ``nack`` rejects a live non-empty batch without advancing the cursor: the
batch's *first* event accrues a consecutive failure and the lease is released
immediately, so the next claim returns the same head event again. When the
failure count for that event reaches ``CONSUMER_MAX_ATTEMPTS``, the same
``BEGIN IMMEDIATE`` transaction writes a dead-letter record and moves the
cursor one step past the offending event; subsequent claims continue from the
next sequence. Retrying a nack with the same settled token replays the
original outcome and never counts twice.

Mutations run inside ``BEGIN IMMEDIATE`` transactions, so concurrent claim
attempts from multiple processes/instances serialise in SQLite and can never
produce two parallel valid leases for one consumer.
"""

from __future__ import annotations

import secrets
import sqlite3
import time
from dataclasses import dataclass

from . import config, db


class LeaseError(Exception):
    """Raised when an ack/nack token is not a valid live lease (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class ClaimedEvent:
    event_id: str
    sequence: int
    delta: int

    def as_dict(self) -> dict:
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "delta": self.delta,
        }


@dataclass(frozen=True)
class Claim:
    stream_id: str
    consumer_id: str
    lease_token: str
    expires_at: int  # POSIX milliseconds
    from_sequence: int
    to_sequence: int  # exclusive upper bound; equals from_sequence for an empty claim
    events: list[ClaimedEvent]

    def as_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "lease_token": self.lease_token,
            "expires_at": self.expires_at,
            "from_sequence": self.from_sequence,
            "to_sequence": self.to_sequence,
            "events": [event.as_dict() for event in self.events],
        }


@dataclass(frozen=True)
class NackResult:
    stream_id: str
    consumer_id: str
    lease_token: str
    cursor: int
    attempts: int
    max_attempts: int
    dead_lettered: bool
    reason: str
    replayed: bool

    def as_dict(self) -> dict:
        return {
            "status": "ok",
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "lease_token": self.lease_token,
            "cursor": self.cursor,
            "attempts": self.attempts,
            "max_attempts": self.max_attempts,
            "dead_lettered": self.dead_lettered,
            "reason": self.reason,
            "replayed": self.replayed,
        }


@dataclass(frozen=True)
class DeadLetter:
    stream_id: str
    consumer_id: str
    sequence: int
    event_id: str
    delta: int
    attempts: int
    last_reason: str
    dead_lettered_at: int  # POSIX milliseconds

    def as_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "sequence": self.sequence,
            "event_id": self.event_id,
            "delta": self.delta,
            "attempts": self.attempts,
            "last_reason": self.last_reason,
            "dead_lettered_at": self.dead_lettered_at,
        }


def _now_ms() -> int:
    # Module-level indirection so tests can fast-forward lease expiry.
    return int(time.time() * 1000)


def _new_lease_token() -> str:
    return secrets.token_urlsafe(24)


def claim(stream_id: str, consumer_id: str, max_events: int) -> Claim | None:
    """Claim the next contiguous applied batch for a consumer.

    Returns ``None`` when the stream does not exist (HTTP 404). While a lease
    is still alive the same claim request returns the same batch and token.
    """
    now_ms = _now_ms()
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT stream_id, next_sequence FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream is None:
            return None
        next_applied = stream["next_sequence"]

        row = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at, lease_from_seq, lease_to_seq "
            "FROM consumers WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        ).fetchone()

        if row is None:
            cursor_seq = 0
            conn.execute(
                "INSERT INTO consumers (stream_id, consumer_id, cursor_seq) VALUES (?, ?, 0)",
                (stream_id, consumer_id),
            )
            lease = None
        else:
            cursor_seq = row["cursor_seq"]
            lease_alive = (
                row["lease_token"] is not None and row["lease_expires_at"] > now_ms
            )
            if lease_alive:
                # Re-claim during an unexpired lease: return the same batch
                # verbatim, including the original token and expiry.
                events = _fetch_events(
                    conn, stream_id, row["lease_from_seq"], row["lease_to_seq"]
                )
                return Claim(
                    stream_id=stream_id,
                    consumer_id=consumer_id,
                    lease_token=row["lease_token"],
                    expires_at=row["lease_expires_at"],
                    from_sequence=row["lease_from_seq"],
                    to_sequence=row["lease_to_seq"],
                    events=events,
                )
            lease = None  # expired or never existed: a fresh lease follows

        # Applied events occupy sequences [0, next_applied); only take a
        # contiguous run starting at the cursor, up to max_events.
        limit = min(max_events, max(0, next_applied - cursor_seq))
        rows = conn.execute(
            "SELECT event_id, sequence, delta FROM events "
            "WHERE stream_id = ? AND sequence >= ? AND sequence < ? "
            "ORDER BY sequence ASC LIMIT ?",
            (stream_id, cursor_seq, next_applied, limit),
        ).fetchall()
        events = [ClaimedEvent(r["event_id"], r["sequence"], r["delta"]) for r in rows]
        to_sequence = cursor_seq + len(events)

        token = _new_lease_token()
        expires_at = now_ms + config.lease_duration_seconds() * 1000
        conn.execute(
            "UPDATE consumers SET lease_token = ?, lease_expires_at = ?, "
            "lease_from_seq = ?, lease_to_seq = ? "
            "WHERE stream_id = ? AND consumer_id = ?",
            (token, expires_at, cursor_seq, to_sequence, stream_id, consumer_id),
        )
        return Claim(
            stream_id=stream_id,
            consumer_id=consumer_id,
            lease_token=token,
            expires_at=expires_at,
            from_sequence=cursor_seq,
            to_sequence=to_sequence,
            events=events,
        )


def _fetch_events(
    conn: sqlite3.Connection, stream_id: str, from_seq: int, to_seq: int
) -> list[ClaimedEvent]:
    if to_seq <= from_seq:
        return []
    rows = conn.execute(
        "SELECT event_id, sequence, delta FROM events "
        "WHERE stream_id = ? AND sequence >= ? AND sequence < ? "
        "ORDER BY sequence ASC",
        (stream_id, from_seq, to_seq),
    ).fetchall()
    return [ClaimedEvent(r["event_id"], r["sequence"], r["delta"]) for r in rows]


def _validate_live_lease(
    conn: sqlite3.Connection,
    stream_id: str,
    consumer_id: str,
    token: str,
    now_ms: int,
    action: str,
) -> sqlite3.Row:
    """Fetch and validate the current lease, mirroring ack's 409 cases.

    ``action`` is ``"ack"`` or ``"nack"`` and only shapes the error message.
    """
    row = conn.execute(
        "SELECT cursor_seq, lease_token, lease_expires_at, lease_from_seq, lease_to_seq "
        "FROM consumers WHERE stream_id = ? AND consumer_id = ?",
        (stream_id, consumer_id),
    ).fetchone()
    if row is None or row["lease_token"] is None:
        raise LeaseError(
            "lease_not_active", f"consumer has no active lease; claim before {action}"
        )
    if secrets.compare_digest(row["lease_token"], token) is False:
        # Wrong token: stale/old lease token, or a forged value.
        raise LeaseError(
            "lease_token_mismatch",
            "token does not match the current lease",
        )
    if row["lease_expires_at"] <= now_ms:
        raise LeaseError(
            "lease_expired",
            "the lease for this token has expired; claim the batch again",
        )
    return row


def _release_lease(conn: sqlite3.Connection, stream_id: str, consumer_id: str) -> None:
    conn.execute(
        "UPDATE consumers SET lease_token = NULL, lease_expires_at = NULL, "
        "lease_from_seq = NULL, lease_to_seq = NULL "
        "WHERE stream_id = ? AND consumer_id = ?",
        (stream_id, consumer_id),
    )


def ack(stream_id: str, consumer_id: str, token: str) -> int | None:
    """Advance the cursor past the batch covered by a live lease token.

    Returns the new cursor sequence, or ``None`` when the stream does not
    exist (HTTP 404). Raises :class:`LeaseError` (HTTP 409) when the token does
    not match the current unexpired lease.
    """
    now_ms = _now_ms()
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if stream is None:
            return None

        row = _validate_live_lease(conn, stream_id, consumer_id, token, now_ms, "ack")

        new_cursor = row["lease_to_seq"]
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL, "
            "lease_expires_at = NULL, lease_from_seq = NULL, lease_to_seq = NULL "
            "WHERE stream_id = ? AND consumer_id = ?",
            (new_cursor, stream_id, consumer_id),
        )
        # Successful delivery resets the consecutive-nack streak of every
        # event the cursor is now skipping (in particular the batch head).
        conn.execute(
            "DELETE FROM consumer_failures "
            "WHERE stream_id = ? AND consumer_id = ? AND sequence < ?",
            (stream_id, consumer_id, new_cursor),
        )
        return new_cursor


def nack(stream_id: str, consumer_id: str, token: str, reason: str) -> NackResult | None:
    """Reject a live non-empty lease batch and release its lease immediately.

    The cursor stays put and the batch's first event accrues one consecutive
    failure; the next claim therefore returns that event again. At
    ``CONSUMER_MAX_ATTEMPTS`` failures the head event is dead-lettered and the
    cursor moves one sequence past it, all in the same ``BEGIN IMMEDIATE``
    transaction.

    A nack retried with a token whose nack was already settled (and before a
    newer lease is claimed) replays the original outcome without counting
    again. Returns ``None`` when the stream does not exist (HTTP 404); raises
    :class:`LeaseError` (HTTP 409) for old/forged/expired/already-nacked
    tokens and for empty batches.
    """
    now_ms = _now_ms()
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if stream is None:
            return None

        row = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at, lease_from_seq, lease_to_seq "
            "FROM consumers WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        ).fetchone()

        if row is None or row["lease_token"] is None:
            # No live lease: this is either a retry of a previously settled
            # nack for this consumer (replay the original result) or an
            # invalid/wrong-consumer token.
            outcome = conn.execute(
                "SELECT lease_token, cursor_seq, attempts, max_attempts, "
                "dead_lettered, reason FROM nack_outcomes "
                "WHERE lease_token = ? AND stream_id = ? AND consumer_id = ?",
                (token, stream_id, consumer_id),
            ).fetchone()
            if outcome is not None:
                return NackResult(
                    stream_id=stream_id,
                    consumer_id=consumer_id,
                    lease_token=outcome["lease_token"],
                    cursor=outcome["cursor_seq"],
                    attempts=outcome["attempts"],
                    max_attempts=outcome["max_attempts"],
                    dead_lettered=bool(outcome["dead_lettered"]),
                    reason=outcome["reason"],
                    replayed=True,
                )
            raise LeaseError(
                "lease_not_active", "consumer has no active lease; claim before nack"
            )

        # A live lease exists: the token must match it and be unexpired. This
        # runs before the replay lookup, so an already-nacked token reused
        # after a fresh claim is rejected as a stale token (409).
        if secrets.compare_digest(row["lease_token"], token) is False:
            raise LeaseError(
                "lease_token_mismatch",
                "token does not match the current lease",
            )
        if row["lease_expires_at"] <= now_ms:
            raise LeaseError(
                "lease_expired",
                "the lease for this token has expired; claim the batch again",
            )

        head_seq = row["lease_from_seq"]
        if head_seq >= row["lease_to_seq"]:
            # Empty batches carry no events and cannot be rejected; the lease
            # is left untouched so the client can claim/ack normally.
            raise LeaseError(
                "empty_batch_not_nackable",
                "an empty claim batch carries no events and cannot be nacked",
            )

        head_event = conn.execute(
            "SELECT event_id, delta FROM events "
            "WHERE stream_id = ? AND sequence = ?",
            (stream_id, head_seq),
        ).fetchone()

        # Count one more consecutive failure against the batch's first event.
        conn.execute(
            "INSERT INTO consumer_failures "
            "(stream_id, consumer_id, sequence, attempts, last_reason, updated_at) "
            "VALUES (?, ?, ?, 1, ?, ?) "
            "ON CONFLICT (stream_id, consumer_id, sequence) DO UPDATE SET "
            "attempts = attempts + 1, last_reason = excluded.last_reason, "
            "updated_at = excluded.updated_at",
            (stream_id, consumer_id, head_seq, reason, now_ms),
        )
        failure = conn.execute(
            "SELECT attempts FROM consumer_failures "
            "WHERE stream_id = ? AND consumer_id = ? AND sequence = ?",
            (stream_id, consumer_id, head_seq),
        ).fetchone()
        attempts = failure["attempts"]
        max_attempts = config.consumer_max_attempts()

        # The lease is released immediately: the batch can be re-claimed at once.
        _release_lease(conn, stream_id, consumer_id)

        if attempts >= max_attempts:
            # Threshold reached: dead-letter the head event and skip past it in
            # the very same transaction so no later claim can hand it out again.
            conn.execute(
                "DELETE FROM consumer_failures "
                "WHERE stream_id = ? AND consumer_id = ? AND sequence = ?",
                (stream_id, consumer_id, head_seq),
            )
            conn.execute(
                "INSERT INTO dead_letters "
                "(stream_id, consumer_id, sequence, event_id, delta, attempts, "
                "last_reason, dead_lettered_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    stream_id,
                    consumer_id,
                    head_seq,
                    head_event["event_id"],
                    head_event["delta"],
                    attempts,
                    reason,
                    now_ms,
                ),
            )
            new_cursor = head_seq + 1
            conn.execute(
                "UPDATE consumers SET cursor_seq = ? "
                "WHERE stream_id = ? AND consumer_id = ?",
                (new_cursor, stream_id, consumer_id),
            )
            dead_lettered = True
        else:
            # Cursor unchanged: the next claim starts at the same head event.
            new_cursor = row["cursor_seq"]
            dead_lettered = False

        # Retain only the latest settled outcome per consumer; the token of an
        # older lease is rejected as lease_token_mismatch once a newer lease
        # exists, so its replay row is no longer needed.
        conn.execute(
            "DELETE FROM nack_outcomes WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        )
        conn.execute(
            "INSERT INTO nack_outcomes "
            "(lease_token, stream_id, consumer_id, cursor_seq, attempts, "
            "max_attempts, dead_lettered, reason, settled_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                token,
                stream_id,
                consumer_id,
                new_cursor,
                attempts,
                max_attempts,
                1 if dead_lettered else 0,
                reason,
                now_ms,
            ),
        )

        return NackResult(
            stream_id=stream_id,
            consumer_id=consumer_id,
            lease_token=token,
            cursor=new_cursor,
            attempts=attempts,
            max_attempts=max_attempts,
            dead_lettered=dead_lettered,
            reason=reason,
            replayed=False,
        )


def list_dead_letters(stream_id: str, consumer_id: str) -> list[DeadLetter] | None:
    """Return a consumer's dead-letter records ordered by sequence ascending.

    Returns ``None`` when the stream does not exist (HTTP 404). A consumer
    that has never claimed anything simply yields an empty list.
    """
    with db.read_transaction() as conn:
        stream = conn.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if stream is None:
            return None
        rows = conn.execute(
            "SELECT stream_id, consumer_id, sequence, event_id, delta, attempts, "
            "last_reason, dead_lettered_at FROM dead_letters "
            "WHERE stream_id = ? AND consumer_id = ? ORDER BY sequence ASC",
            (stream_id, consumer_id),
        ).fetchall()
        return [
            DeadLetter(
                stream_id=r["stream_id"],
                consumer_id=r["consumer_id"],
                sequence=r["sequence"],
                event_id=r["event_id"],
                delta=r["delta"],
                attempts=r["attempts"],
                last_reason=r["last_reason"],
                dead_lettered_at=r["dead_lettered_at"],
            )
            for r in rows
        ]
