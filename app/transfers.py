"""Cross-stream atomic transfers: one debit event plus one credit event.

A transfer request carries a client-assigned ``transfer_id`` (the global
idempotency key), a distinct ``source_stream_id``/``target_stream_id`` pair and
a positive integer ``amount``. Both streams must already exist, and the source
stream's current balance must cover the amount.

On success a debit event (``delta = -amount``) is appended at the source
stream's ``next_sequence`` and a credit event (``delta = +amount``) at the
target stream's ``next_sequence``, each carrying a deterministic
``transfer:<transfer_id>:<side>`` event id. Both events are then run through
the ordinary contiguous catch-up (the append may also unblock a buffered run
of previously gap-parked events), and the two streams' ``balance`` /
``next_sequence`` rows are updated — all inside one ``BEGIN IMMEDIATE``
transaction. Any failure rolls the whole operation back: no events and no
balance changes survive.

A retried request with the same ``transfer_id`` and identical parameters
returns the stored original result (HTTP 200) and never debits twice; same
``transfer_id`` with different parameters is a ``409``. Concurrent retries
serialise on the write lock, so exactly one transfer is ever committed.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from . import db, repo


class TransferError(Exception):
    """Raised when a transfer request is invalid or clashes (HTTP 409/422)."""

    def __init__(self, code: str, message: str, status_code: int = 409):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code


@dataclass(frozen=True)
class TransferResult:
    transfer_id: str
    source_stream_id: str
    target_stream_id: str
    amount: int
    source_event_id: str
    source_sequence: int
    target_event_id: str
    target_sequence: int
    source: repo.StreamState
    target: repo.StreamState
    replayed: bool


def _debit_event_id(transfer_id: str) -> str:
    return f"transfer:{transfer_id}:debit"


def _credit_event_id(transfer_id: str) -> str:
    return f"transfer:{transfer_id}:credit"


def _append_event(
    conn: sqlite3.Connection,
    stream_id: str,
    event_id: str,
    sequence: int,
    delta: int,
) -> None:
    # The sequence slot is the stream's next_sequence, which is always free;
    # the event id is namespaced by transfer_id. The UNIQUE constraints are
    # defence in depth for any collision.
    conn.execute(
        "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
        "VALUES (?, ?, ?, ?, 0)",
        (stream_id, event_id, sequence, delta),
    )


def create_transfer(
    transfer_id: str,
    source_stream_id: str,
    target_stream_id: str,
    amount: int,
) -> TransferResult:
    """Debit ``amount`` from the source stream and credit it to the target.

    Returns the created/existing :class:`TransferResult`. Raises
    :class:`TransferError` for a missing source/target stream (HTTP 404),
    insufficient funds (HTTP 422) or a clashing idempotent replay (HTTP 409).
    """
    with db.write_transaction() as conn:
        existing = conn.execute(
            "SELECT transfer_id, source_stream_id, target_stream_id, amount, "
            "source_sequence, target_sequence FROM transfers WHERE transfer_id = ?",
            (transfer_id,),
        ).fetchone()
        if existing is not None:
            # Idempotent replay: same transfer_id must carry identical parameters.
            if (
                existing["source_stream_id"] != source_stream_id
                or existing["target_stream_id"] != target_stream_id
                or existing["amount"] != amount
            ):
                raise TransferError(
                    "transfer_replay_conflict",
                    f"transfer_id {transfer_id!r} already exists with different "
                    "source/target/amount",
                )
            # Return the original event coordinates, but the *current* state of
            # both streams (later events may have been applied since the transfer).
            source_row = _get_stream_row(conn, existing["source_stream_id"])
            target_row = _get_stream_row(conn, existing["target_stream_id"])
            return TransferResult(
                transfer_id=transfer_id,
                source_stream_id=existing["source_stream_id"],
                target_stream_id=existing["target_stream_id"],
                amount=existing["amount"],
                source_event_id=_debit_event_id(transfer_id),
                source_sequence=existing["source_sequence"],
                target_event_id=_credit_event_id(transfer_id),
                target_sequence=existing["target_sequence"],
                source=repo.build_state(
                    conn,
                    existing["source_stream_id"],
                    source_row["balance"],
                    source_row["next_sequence"],
                ),
                target=repo.build_state(
                    conn,
                    existing["target_stream_id"],
                    target_row["balance"],
                    target_row["next_sequence"],
                ),
                replayed=True,
            )

        source = _get_stream_row(conn, source_stream_id)
        if source is None:
            raise TransferError(
                "source_stream_not_found",
                f"source stream {source_stream_id!r} does not exist",
                status_code=404,
            )
        target = _get_stream_row(conn, target_stream_id)
        if target is None:
            raise TransferError(
                "target_stream_not_found",
                f"target stream {target_stream_id!r} does not exist",
                status_code=404,
            )

        # A sealed stream can never gain another event; the replay branch
        # above returned before this point, so transfers completed before the
        # seal remain idempotently replayable.
        source_sealed = repo.is_sealed(conn, source_stream_id)
        target_sealed = repo.is_sealed(conn, target_stream_id)
        if source_sealed or target_sealed:
            sealed = source_stream_id if source_sealed else target_stream_id
            raise TransferError(
                "stream_sealed",
                f"stream {sealed!r} is sealed and cannot accept new events",
            )

        # Balance reflects exactly the applied prefix [0, next_sequence); the
        # pending buffered events are not spendable until their gap closes.
        if source["balance"] < amount:
            raise TransferError(
                "insufficient_funds",
                f"source stream {source_stream_id!r} balance {source['balance']} "
                f"is less than transfer amount {amount}",
                status_code=422,
            )

        source_seq = source["next_sequence"]
        target_seq = target["next_sequence"]
        source_balance = source["balance"]
        target_balance = target["balance"]

        # Append both ledger events first, then run each side's contiguous
        # catch-up inside the same transaction.
        _append_event(
            conn, source_stream_id, _debit_event_id(transfer_id), source_seq, -amount
        )
        _append_event(
            conn, target_stream_id, _credit_event_id(transfer_id), target_seq, amount
        )

        source_balance, source_next = repo.apply_contiguous(
            conn, source_stream_id, source_balance, source_seq
        )
        target_balance, target_next = repo.apply_contiguous(
            conn, target_stream_id, target_balance, target_seq
        )

        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (source_balance, source_next, source_stream_id),
        )
        conn.execute(
            "UPDATE streams SET balance = ?, next_sequence = ? WHERE stream_id = ?",
            (target_balance, target_next, target_stream_id),
        )
        conn.execute(
            "INSERT INTO transfers (transfer_id, source_stream_id, target_stream_id, "
            "amount, source_sequence, target_sequence) VALUES (?, ?, ?, ?, ?, ?)",
            (
                transfer_id,
                source_stream_id,
                target_stream_id,
                amount,
                source_seq,
                target_seq,
            ),
        )

        return TransferResult(
            transfer_id=transfer_id,
            source_stream_id=source_stream_id,
            target_stream_id=target_stream_id,
            amount=amount,
            source_event_id=_debit_event_id(transfer_id),
            source_sequence=source_seq,
            target_event_id=_credit_event_id(transfer_id),
            target_sequence=target_seq,
            source=repo.build_state(conn, source_stream_id, source_balance, source_next),
            target=repo.build_state(conn, target_stream_id, target_balance, target_next),
            replayed=False,
        )


def _get_stream_row(
    conn: sqlite3.Connection, stream_id: str
) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT stream_id, balance, next_sequence FROM streams WHERE stream_id = ?",
        (stream_id,),
    ).fetchone()
