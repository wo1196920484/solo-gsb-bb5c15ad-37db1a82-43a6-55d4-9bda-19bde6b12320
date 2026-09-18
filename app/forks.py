"""Stream forking: copy an applied-event prefix into a new appendable stream.

A fork request carries a client-assigned ``fork_id`` (the idempotency key
within a source stream), a ``target_stream_id`` and an exclusive upper bound
``through_sequence``. The applied events of the source stream occupying
sequences ``[0, through_sequence)`` are copied verbatim (already applied) into
a freshly created target stream whose ``balance`` is the sum of their deltas
and whose ``next_sequence`` is ``through_sequence``; events submitted to the
target afterwards simply continue the sequence numbering.

The whole operation — fork record, target stream row and copied events — is
written inside one ``BEGIN IMMEDIATE`` transaction, so concurrent requests
serialise in SQLite and can never leave half-built streams behind. A retried
request with the same ``(source, fork_id)`` and identical parameters returns
the existing fork; same ``fork_id`` with different parameters is a ``409``.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import db


class ForkError(Exception):
    """Raised when a fork request is invalid or clashes (HTTP 409/422)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class Fork:
    source_stream_id: str
    fork_id: str
    target_stream_id: str
    through_sequence: int
    balance: int
    next_sequence: int
    replayed: bool

    def as_dict(self) -> dict:
        return {
            "source_stream_id": self.source_stream_id,
            "target_stream_id": self.target_stream_id,
            "through_sequence": self.through_sequence,
            "balance": self.balance,
            "next_sequence": self.next_sequence,
            "replayed": self.replayed,
        }


def create_fork(
    source_stream_id: str,
    fork_id: str,
    target_stream_id: str,
    through_sequence: int,
) -> Fork | None:
    """Fork ``[0, through_sequence)`` applied events into a new stream.

    Returns the created/existing :class:`Fork`, or ``None`` when the source
    stream does not exist (HTTP 404). Raises :class:`ForkError` for clashing
    forks (HTTP 409) or an out-of-range ``through_sequence`` (HTTP 422).
    """
    with db.write_transaction() as conn:
        source = conn.execute(
            "SELECT stream_id, next_sequence FROM streams WHERE stream_id = ?",
            (source_stream_id,),
        ).fetchone()
        if source is None:
            return None

        existing = conn.execute(
            "SELECT fork_id, target_stream_id, through_sequence, balance "
            "FROM forks WHERE source_stream_id = ? AND fork_id = ?",
            (source_stream_id, fork_id),
        ).fetchone()
        if existing is not None:
            # Idempotent replay: same fork_id must carry identical parameters.
            if (
                existing["target_stream_id"] != target_stream_id
                or existing["through_sequence"] != through_sequence
            ):
                raise ForkError(
                    "fork_replay_conflict",
                    f"fork_id {fork_id!r} already exists for stream "
                    f"{source_stream_id!r} with different parameters",
                )
            return Fork(
                source_stream_id=source_stream_id,
                fork_id=fork_id,
                target_stream_id=existing["target_stream_id"],
                through_sequence=existing["through_sequence"],
                balance=existing["balance"],
                next_sequence=existing["through_sequence"],
                replayed=True,
            )

        # Applied events occupy exactly [0, next_sequence); the exclusive cut
        # point must sit within that range.
        if not 0 <= through_sequence <= source["next_sequence"]:
            raise ForkError(
                "through_sequence_out_of_range",
                f"through_sequence must be in [0, {source['next_sequence']}] "
                f"(stream next_sequence), got {through_sequence}",
                status_code=422,
            )

        target = conn.execute(
            "SELECT stream_id FROM streams WHERE stream_id = ?",
            (target_stream_id,),
        ).fetchone()
        if target is not None:
            raise ForkError(
                "target_stream_exists",
                f"target stream {target_stream_id!r} already exists",
            )

        rows = conn.execute(
            "SELECT event_id, sequence, delta FROM events "
            "WHERE stream_id = ? AND sequence >= 0 AND sequence < ? AND applied = 1 "
            "ORDER BY sequence ASC",
            (source_stream_id, through_sequence),
        ).fetchall()
        balance = sum(row["delta"] for row in rows)

        # Target stream first: the copied events reference it via the FK.
        conn.execute(
            "INSERT INTO streams (stream_id, balance, next_sequence) VALUES (?, ?, ?)",
            (target_stream_id, balance, through_sequence),
        )
        if rows:
            conn.executemany(
                "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
                "VALUES (?, ?, ?, ?, 1)",
                [
                    (target_stream_id, row["event_id"], row["sequence"], row["delta"])
                    for row in rows
                ],
            )
        conn.execute(
            "INSERT INTO forks (source_stream_id, fork_id, target_stream_id, "
            "through_sequence, balance) VALUES (?, ?, ?, ?, ?)",
            (
                source_stream_id,
                fork_id,
                target_stream_id,
                through_sequence,
                balance,
            ),
        )

        return Fork(
            source_stream_id=source_stream_id,
            fork_id=fork_id,
            target_stream_id=target_stream_id,
            through_sequence=through_sequence,
            balance=balance,
            next_sequence=through_sequence,
            replayed=False,
        )
