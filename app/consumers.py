"""Consumer cursors and claim/ack leases for event consumption.

Each ``(stream_id, consumer_id)`` owns a cursor (``cursor_seq``): the sequence
of the next event the consumer needs to receive. A ``claim`` hands out up to
``max_events`` contiguous *applied* events starting at the cursor and records a
time-limited lease on that exact batch. An ``ack`` carrying the lease token
atomically advances the cursor past the batch, but only while the lease is
alive. After expiry the same batch may be claimed again (at-least-once
delivery).

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
    """Raised when an ack token is not a valid live lease (HTTP 409)."""

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
            "lease_expires_at = NULL, lease_from_seq = NULL, lease_to_seq = NULL "
            "WHERE stream_id = ? AND consumer_id = ?",
            (new_cursor, stream_id, consumer_id),
        )
        return new_cursor
