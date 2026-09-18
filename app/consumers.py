"""Consumer cursors and claim/ack/nack leases for event consumption.

Each ``(stream_id, consumer_id)`` owns a cursor (``cursor_seq``): the sequence
of the next event the consumer needs to receive. A ``claim`` hands out up to
``max_events`` contiguous *applied* events starting at the cursor and records a
time-limited lease on that exact batch. An ``ack`` carrying the lease token
atomically advances the cursor past the batch, but only while the lease is
alive. After expiry the same batch may be claimed again (at-least-once
delivery).

A ``nack`` rejects a claimed batch without advancing the cursor. The lease is
released immediately and the consecutive-failure counter of the batch's *head*
event (``failure_seq``/``failure_count`` on the consumer row) is incremented;
while it stays below ``CONSUMER_MAX_ATTEMPTS`` the next claim redelivers the
same event from the unchanged cursor. When the counter reaches the threshold a
dead-letter record is written and the cursor skips the head event — both in the
same ``BEGIN IMMEDIATE`` transaction — so later claims continue at the next
sequence. Empty batches cannot be nacked. Each consumed lease token is stored
in ``consumer_nacks``: replaying a nack with the same token returns the original
outcome and never counts twice, while old, forged or already-consumed tokens
get ``409`` like ack.

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

        row = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at, lease_to_seq "
            "FROM consumers WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        ).fetchone()
        if row is None or row["lease_token"] is None:
            raise LeaseError(
                "lease_not_active", "consumer has no active lease; claim before ack"
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

        new_cursor = row["lease_to_seq"]
        conn.execute(
            "UPDATE consumers SET cursor_seq = ?, lease_token = NULL, "
            "lease_expires_at = NULL, lease_from_seq = NULL, lease_to_seq = NULL, "
            "failure_seq = NULL, failure_count = NULL "
            "WHERE stream_id = ? AND consumer_id = ?",
            (new_cursor, stream_id, consumer_id),
        )
        return new_cursor


@dataclass(frozen=True)
class NackResult:
    stream_id: str
    consumer_id: str
    head_sequence: int
    attempt: int  # consecutive failure count of the head event after this nack
    max_attempts: int
    dead_lettered: bool
    cursor: int  # cursor after the nack (unchanged below the threshold, +1 when dead-lettered)
    replayed: bool

    def as_dict(self) -> dict:
        return {
            "status": "ok",
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "head_sequence": self.head_sequence,
            "attempt": self.attempt,
            "max_attempts": self.max_attempts,
            "dead_lettered": self.dead_lettered,
            "cursor": self.cursor,
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
    quarantined_at: int  # POSIX milliseconds

    def as_dict(self) -> dict:
        return {
            "stream_id": self.stream_id,
            "consumer_id": self.consumer_id,
            "sequence": self.sequence,
            "event_id": self.event_id,
            "delta": self.delta,
            "attempts": self.attempts,
            "last_reason": self.last_reason,
            "quarantined_at": self.quarantined_at,
        }


def nack(
    stream_id: str, consumer_id: str, token: str, reason: str
) -> NackResult | None:
    """Reject the batch covered by a live lease token.

    The cursor is not advanced; the lease is released immediately. The
    consecutive-failure counter of the batch's head event is incremented, and
    once it reaches ``CONSUMER_MAX_ATTEMPTS`` a dead-letter record is written
    and the cursor skips the head event in the same transaction.

    Returns ``None`` when the stream does not exist (HTTP 404). Raises
    :class:`LeaseError` (HTTP 409) for an old/forged/consumed token, an expired
    lease or an empty batch. Replaying the same token returns the stored
    original outcome (``replayed=True``) without counting again.
    """
    now_ms = _now_ms()
    max_attempts = config.consumer_max_attempts()
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if stream is None:
            return None

        # Idempotent replay takes precedence: a token that was already used to
        # nack always returns its original outcome, even though the lease it
        # referred to has long been released. The ledger row is bound to this
        # stream/consumer: presenting the token elsewhere is a forged token.
        prior = conn.execute(
            "SELECT head_sequence, attempt, dead_lettered FROM consumer_nacks "
            "WHERE lease_token = ? AND stream_id = ? AND consumer_id = ?",
            (token, stream_id, consumer_id),
        ).fetchone()
        if prior is not None:
            consumer = conn.execute(
                "SELECT cursor_seq FROM consumers WHERE stream_id = ? AND consumer_id = ?",
                (stream_id, consumer_id),
            ).fetchone()
            # The consumer row always exists (the original nack created it);
            # derive the cursor defensively if it is ever missing.
            if consumer is not None:
                replay_cursor = consumer["cursor_seq"]
            elif prior["dead_lettered"]:
                replay_cursor = prior["head_sequence"] + 1
            else:
                replay_cursor = prior["head_sequence"]
            return NackResult(
                stream_id=stream_id,
                consumer_id=consumer_id,
                head_sequence=prior["head_sequence"],
                attempt=prior["attempt"],
                max_attempts=max_attempts,
                dead_lettered=bool(prior["dead_lettered"]),
                cursor=replay_cursor,
                replayed=True,
            )

        row = conn.execute(
            "SELECT cursor_seq, lease_token, lease_expires_at, lease_from_seq, "
            "lease_to_seq, failure_seq, failure_count "
            "FROM consumers WHERE stream_id = ? AND consumer_id = ?",
            (stream_id, consumer_id),
        ).fetchone()
        if row is None or row["lease_token"] is None:
            raise LeaseError(
                "lease_not_active", "consumer has no active lease; claim before nack"
            )
        if secrets.compare_digest(row["lease_token"], token) is False:
            # Wrong token: stale/old lease token, or a forged value. It may also
            # have already been consumed by ack or by a *different* nack token.
            raise LeaseError(
                "lease_token_mismatch",
                "token does not match the current lease",
            )
        if row["lease_expires_at"] <= now_ms:
            raise LeaseError(
                "lease_expired",
                "the lease for this token has expired; claim the batch again",
            )
        if row["lease_to_seq"] <= row["lease_from_seq"]:
            raise LeaseError(
                "empty_batch", "an empty claim batch cannot be nacked"
            )

        head_sequence = row["lease_from_seq"]
        if row["failure_seq"] == head_sequence:
            attempt = row["failure_count"] + 1
        else:
            # First recorded failure of this head event (previous events were
            # acknowledged, or this is the first nack at the cursor).
            attempt = 1

        dead_lettered = attempt >= max_attempts

        # The lease is always released immediately; a fresh claim decides
        # whether the head event is redelivered (cursor unchanged) or skipped.
        if dead_lettered:
            event = conn.execute(
                "SELECT event_id, delta FROM events "
                "WHERE stream_id = ? AND sequence = ?",
                (stream_id, head_sequence),
            ).fetchone()
            conn.execute(
                "INSERT INTO dead_letters "
                "(stream_id, consumer_id, sequence, event_id, delta, attempts, "
                "last_reason, quarantined_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    stream_id,
                    consumer_id,
                    head_sequence,
                    event["event_id"],
                    event["delta"],
                    attempt,
                    reason,
                    now_ms,
                ),
            )
            conn.execute(
                "UPDATE consumers SET cursor_seq = ?, lease_token = NULL, "
                "lease_expires_at = NULL, lease_from_seq = NULL, lease_to_seq = NULL, "
                "failure_seq = NULL, failure_count = NULL "
                "WHERE stream_id = ? AND consumer_id = ?",
                (head_sequence + 1, stream_id, consumer_id),
            )
        else:
            conn.execute(
                "UPDATE consumers SET lease_token = NULL, lease_expires_at = NULL, "
                "lease_from_seq = NULL, lease_to_seq = NULL, "
                "failure_seq = ?, failure_count = ? "
                "WHERE stream_id = ? AND consumer_id = ?",
                (head_sequence, attempt, stream_id, consumer_id),
            )

        conn.execute(
            "INSERT INTO consumer_nacks "
            "(stream_id, consumer_id, lease_token, head_sequence, attempt, dead_lettered) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                stream_id,
                consumer_id,
                token,
                head_sequence,
                attempt,
                1 if dead_lettered else 0,
            ),
        )
        return NackResult(
            stream_id=stream_id,
            consumer_id=consumer_id,
            head_sequence=head_sequence,
            attempt=attempt,
            max_attempts=max_attempts,
            dead_lettered=dead_lettered,
            cursor=head_sequence + 1 if dead_lettered else head_sequence,
            replayed=False,
        )


def list_dead_letters(
    stream_id: str, consumer_id: str
) -> list[DeadLetter] | None:
    """Return a consumer's dead-letter records in ascending sequence order.

    Returns ``None`` when the stream does not exist (HTTP 404). A consumer that
    has never dead-lettered anything yields an empty list.
    """
    with db.read_transaction() as conn:
        stream = conn.execute(
            "SELECT 1 FROM streams WHERE stream_id = ?", (stream_id,)
        ).fetchone()
        if stream is None:
            return None
        rows = conn.execute(
            "SELECT stream_id, consumer_id, sequence, event_id, delta, attempts, "
            "last_reason, quarantined_at FROM dead_letters "
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
                quarantined_at=r["quarantined_at"],
            )
            for r in rows
        ]
