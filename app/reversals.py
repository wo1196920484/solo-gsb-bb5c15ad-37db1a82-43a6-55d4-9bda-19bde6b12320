"""Event reversals: undo an applied event by appending a compensation event.

A reversal request targets one *applied and not yet reversed* event of a
stream and carries a client-assigned ``reversal_id`` (the **global**
idempotency key). On success a compensation event with deterministic event id
``reversal:<reversal_id>`` and ``delta = -<original delta>`` is appended at
the stream's current ``next_sequence``, run through the ordinary contiguous
catch-up (the append may also unblock a buffered run of previously gap-parked
events), the stream row is updated and a ``reversals`` ledger row is written —
all inside one ``BEGIN IMMEDIATE`` transaction. Any failure rolls the whole
operation back: no event, no balance change and no reversal record survives.

``reversal_id`` is globally unique: a retry against the same
``(stream_id, event_id)`` returns the stored original coordinates plus the
*current* stream state (HTTP 200) and never compensates twice; the same
``reversal_id`` against a different target is a ``409``. Reversing an event
that was already reversed (under any other ``reversal_id``) is also a ``409``
— the ``UNIQUE (stream_id, event_id)`` constraint guarantees at most one
reversal per original event. Concurrent requests serialise on the write lock,
so exactly one compensation event is ever appended.

The compensation event is an ordinary applied event: it is visible through
``GET /streams/{id}``, the consumer claim/ack API and stream forks.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import db, repo


class ReversalError(Exception):
    """Raised when a reversal request is invalid or clashes (HTTP 404/409)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class ReversalResult:
    reversal_id: str
    stream_id: str
    event_id: str  # the original event being reversed
    reversal_event_id: str  # the appended compensation event
    sequence: int  # sequence the compensation event was appended at
    delta: int  # compensation delta (negated original delta)
    state: repo.StreamState
    replayed: bool


def _reversal_event_id(reversal_id: str) -> str:
    return f"reversal:{reversal_id}"


def _get_stream_row(
    conn: sqlite3.Connection, stream_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
        (stream_id,),
    ).fetchone()


def create_reversal(
    stream_id: str, event_id: str, reversal_id: str
) -> ReversalResult:
    """Append a compensation event undoing ``event_id`` of ``stream_id``.

    Returns the created/existing :class:`ReversalResult`. Raises
    :class:`ReversalError` for a missing stream/event (HTTP 404), an event
    that has not been applied yet or was already reversed (HTTP 409), or a
    clashing idempotent replay (HTTP 409).
    """
    with db.write_transaction() as conn:
        existing = conn.execute(
            "SELECT reversal_id, stream_id, event_id, reversal_event_id, "
            "reversal_sequence, delta FROM reversals WHERE reversal_id = ?",
            (reversal_id,),
        ).fetchone()
        if existing is not None:
            # Idempotent replay: same reversal_id must target the same event.
            if existing["stream_id"] != stream_id or existing["event_id"] != event_id:
                raise ReversalError(
                    "reversal_replay_conflict",
                    f"reversal_id {reversal_id!r} already exists for a different "
                    "stream/event",
                )
            # Return the original event coordinates, but the *current* state
            # of the stream (later events may have been applied since).
            stream_row = _get_stream_row(conn, existing["stream_id"])
            return ReversalResult(
                reversal_id=reversal_id,
                stream_id=existing["stream_id"],
                event_id=existing["event_id"],
                reversal_event_id=existing["reversal_event_id"],
                sequence=existing["reversal_sequence"],
                delta=existing["delta"],
                state=repo.build_state(
                    conn,
                    existing["stream_id"],
                    stream_row["balance"],
                    stream_row["next_sequence"],
                ),
                replayed=True,
            )

        stream = _get_stream_row(conn, stream_id)
        if stream is None:
            raise ReversalError(
                "stream_not_found",
                f"unknown stream {stream_id!r}",
                status_code=404,
            )

        # Sealing is irreversible: a sealed stream rejects new compensation
        # events. Reversals completed before the seal take the replay branch
        # above and still return their original result.
        if repo.is_sealed(conn, stream_id):
            raise ReversalError(
                "stream_sealed",
                f"stream {stream_id!r} is sealed and cannot accept new events",
            )

        event = conn.execute(
            "SELECT event_id, sequence, delta, applied "
            "FROM events WHERE stream_id = ? AND event_id = ?",
            (stream_id, event_id),
        ).fetchone()
        if event is None:
            raise ReversalError(
                "event_not_found",
                f"unknown event {event_id!r} in stream {stream_id!r}",
                status_code=404,
            )

        # Only events folded into the balance can be undone; a buffered event
        # (parked behind a gap) has no applied effect to compensate.
        if event["applied"] != 1:
            raise ReversalError(
                "event_not_applied",
                f"event {event_id!r} has not been applied yet "
                "(it is buffered behind a sequence gap)",
            )

        prior = conn.execute(
            "SELECT reversal_id FROM reversals WHERE stream_id = ? AND event_id = ?",
            (stream_id, event_id),
        ).fetchone()
        if prior is not None:
            raise ReversalError(
                "event_already_reversed",
                f"event {event_id!r} was already reversed by reversal_id "
                f"{prior['reversal_id']!r}",
            )

        # The sequence slot is the stream's next_sequence, which is always
        # free; the event id is namespaced by reversal_id. The UNIQUE
        # constraints are defence in depth for any collision.
        sequence = stream["next_sequence"]
        balance = stream["balance"]
        compensation = -event["delta"]
        reversal_event_id = _reversal_event_id(reversal_id)

        # Append the compensation event first, then run the contiguous
        # catch-up and record the reversal inside the same transaction.
        conn.execute(
            "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
            "VALUES (?, ?, ?, ?, 0)",
            (stream_id, reversal_event_id, sequence, compensation),
        )
        balance, next_sequence = repo.apply_contiguous(
            conn, stream_id, balance, sequence
        )
        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (balance, next_sequence, stream_id),
        )
        conn.execute(
            "INSERT INTO reversals (reversal_id, stream_id, event_id, "
            "reversal_event_id, reversal_sequence, delta) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                reversal_id,
                stream_id,
                event_id,
                reversal_event_id,
                sequence,
                compensation,
            ),
        )

        return ReversalResult(
            reversal_id=reversal_id,
            stream_id=stream_id,
            event_id=event_id,
            reversal_event_id=reversal_event_id,
            sequence=sequence,
            delta=compensation,
            state=repo.build_state(conn, stream_id, balance, next_sequence),
            replayed=False,
        )
