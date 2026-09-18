"""Irreversible stream sealing with a content hash of the applied prefix.

A seal request carries a client-assigned ``seal_id`` (the idempotency key
within the stream) and ``expected_next_sequence`` (the expected current
version). The seal is created only when the stream exists, has **no buffered
(pending) events** and its ``next_sequence`` equals the expected version.

Inside one ``BEGIN IMMEDIATE`` transaction, every *applied* event (the applied
prefix occupies exactly ``[0, next_sequence)``) is read in sequence order and
each ``[sequence, event_id, delta]`` triple is rendered as one canonical,
whitespace-free UTF-8 JSON line: ``json.dumps(..., ensure_ascii=False,
separators=(",", ":"))`` preserves non-ASCII characters literally. The lines
are joined with ``"\\n"`` (no trailing newline) and SHA-256'd; the digest plus
the sealed version is stored in the ``seals`` table.

Sealing is irreversible. A retried request with the same ``seal_id`` and
``expected_next_sequence`` returns the stored original result (HTTP 200);
either parameter drifting against an existing seal is a ``409``. A first-time
seal against a stale version or a stream that still has buffered events is
also a ``409`` — nothing is written. Concurrent requests serialise on the
write lock, so a stream is sealed exactly once.

After sealing, operations that would append events (plain event submission,
transfers touching the stream on either side, reversals) are rejected, while
idempotent replays of operations that completed *before* the seal still
return their original result. Querying, claim/ack consumption and using the
sealed stream as a fork source remain available.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
from dataclasses import dataclass

from . import db


class SealError(Exception):
    """Raised when a seal request clashes with the stored state (HTTP 409)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class SealResult:
    stream_id: str
    seal_id: str
    next_sequence: int  # the sealed version (applied-prefix length)
    event_count: int  # number of hashed events
    sha256: str  # hex digest of the canonical newline-joined JSON
    replayed: bool


def _canonical_line(sequence: int, event_id: str, delta: int) -> bytes:
    """Render ``[sequence, event_id, delta]`` as compact UTF-8 JSON.

    ``ensure_ascii=False`` keeps non-ASCII code points literal; the compact
    separators guarantee no insignificant whitespace. The key order is fixed
    by the list layout (sequence, event_id, delta).
    """
    return json.dumps(
        [sequence, event_id, delta],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def create_seal(
    stream_id: str, seal_id: str, expected_next_sequence: int
) -> SealResult | None:
    """Seal ``stream_id`` at ``expected_next_sequence`` and hash its events.

    Returns the created/existing :class:`SealResult`, or ``None`` when the
    stream does not exist (HTTP 404). Raises :class:`SealError` (HTTP 409) for
    a version mismatch, buffered pending events, or a clashing idempotent
    replay.
    """
    with db.write_transaction() as conn:
        stream = conn.execute(
            "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream is None:
            return None

        existing = conn.execute(
            "SELECT seal_id, next_sequence, event_count, sha256 "
            "FROM seals WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if existing is not None:
            # The seal is immutable: the same request replays, any drift 409s.
            if (
                existing["seal_id"] != seal_id
                or existing["next_sequence"] != expected_next_sequence
            ):
                raise SealError(
                    "seal_replay_conflict",
                    f"stream {stream_id!r} is already sealed with seal_id "
                    f"{existing['seal_id']!r} at next_sequence "
                    f"{existing['next_sequence']}",
                )
            return SealResult(
                stream_id=stream_id,
                seal_id=existing["seal_id"],
                next_sequence=existing["next_sequence"],
                event_count=existing["event_count"],
                sha256=existing["sha256"],
                replayed=True,
            )

        # Optimistic version check: the request must agree with the current
        # applied-prefix length. Checked before the pending condition so a
        # stale client gets the version error even when events are buffered.
        if stream["next_sequence"] != expected_next_sequence:
            raise SealError(
                "version_mismatch",
                f"expected next_sequence {expected_next_sequence} but stream "
                f"{stream_id!r} is at {stream['next_sequence']}",
            )

        pending = conn.execute(
            "SELECT 1 FROM events WHERE stream_id = ? AND applied = 0 LIMIT 1",
            (stream_id,),
        ).fetchone()
        if pending is not None:
            raise SealError(
                "stream_has_pending_events",
                f"stream {stream_id!r} has buffered events behind a sequence "
                "gap; close the gap before sealing",
            )

        # Applied events occupy exactly [0, next_sequence); hash them in
        # sequence order as one compact JSON line each.
        rows = conn.execute(
            "SELECT event_id, sequence, delta FROM events "
            "WHERE stream_id = ? AND applied = 1 ORDER BY sequence ASC",
            (stream_id,),
        ).fetchall()
        lines = [
            _canonical_line(row["sequence"], row["event_id"], row["delta"])
            for row in rows
        ]
        digest = hashlib.sha256(b"\n".join(lines)).hexdigest()

        conn.execute(
            "INSERT INTO seals (stream_id, seal_id, next_sequence, event_count, sha256) "
            "VALUES (?, ?, ?, ?, ?)",
            (stream_id, seal_id, stream["next_sequence"], len(rows), digest),
        )

        return SealResult(
            stream_id=stream_id,
            seal_id=seal_id,
            next_sequence=stream["next_sequence"],
            event_count=len(rows),
            sha256=digest,
            replayed=False,
        )
