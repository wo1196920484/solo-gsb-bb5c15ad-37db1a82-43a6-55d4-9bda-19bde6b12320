"""SQLite connection management and schema initialisation."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from . import config

# Give a busy writer up to 5s to clear (WAL permits one writer + many readers).
BUSY_TIMEOUT_SECONDS = 5


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.database_path(),
        timeout=BUSY_TIMEOUT_SECONDS,
        isolation_level=None,  # autocommit mode: transactions are controlled explicitly
    )
    conn.row_factory = sqlite3.Row
    # Enforce declared types (booleans etc. must not sneak into INTEGER columns).
    conn.execute("PRAGMA strict_types = ON")
    return conn


def _apply_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute(f"PRAGMA busy_timeout = {BUSY_TIMEOUT_SECONDS * 1000}")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA synchronous = NORMAL")


def initialise() -> None:
    """Create the database file, enable WAL and create the schema."""
    db_file = Path(config.database_path())
    db_file.parent.mkdir(parents=True, exist_ok=True)

    conn = _connect()
    try:
        _apply_pragmas(conn)
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS streams (
                stream_id     TEXT PRIMARY KEY,
                balance       INTEGER NOT NULL DEFAULT 0,
                next_sequence INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS events (
                stream_id TEXT    NOT NULL,
                event_id  TEXT    NOT NULL,
                sequence  INTEGER NOT NULL CHECK (sequence >= 0),
                delta     INTEGER NOT NULL,
                applied   INTEGER NOT NULL DEFAULT 0 CHECK (applied IN (0, 1)),
                PRIMARY KEY (stream_id, event_id),
                UNIQUE (stream_id, sequence),
                FOREIGN KEY (stream_id) REFERENCES streams(stream_id)
            );

            -- Only one event may occupy each sequence of a stream.
            CREATE INDEX IF NOT EXISTS idx_events_stream_seq
                ON events (stream_id, sequence);
            -- Fast lookup of the contiguous events that catch-up has to apply.
            CREATE INDEX IF NOT EXISTS idx_events_stream_applied_seq
                ON events (stream_id, applied, sequence);

            -- One row per (source stream, fork_id): records the fork cut point
            -- and the target stream that received the copied prefix. The pair
            -- is the idempotency key for retried fork requests; target_stream_id
            -- is unique so two forks can never land on the same target stream.
            CREATE TABLE IF NOT EXISTS forks (
                source_stream_id TEXT    NOT NULL,
                fork_id          TEXT    NOT NULL,
                target_stream_id TEXT    NOT NULL,
                through_sequence INTEGER NOT NULL CHECK (through_sequence >= 0),
                balance          INTEGER NOT NULL,
                PRIMARY KEY (source_stream_id, fork_id),
                UNIQUE (target_stream_id),
                FOREIGN KEY (source_stream_id) REFERENCES streams(stream_id),
                FOREIGN KEY (target_stream_id) REFERENCES streams(stream_id)
            );

            -- One row per completed cross-stream transfer: the idempotency key
            -- is transfer_id alone, and the stored parameters let retries be
            -- compared. The sequences record where the debit/credit events were
            -- appended so an idempotent replay can return the original result.
            CREATE TABLE IF NOT EXISTS transfers (
                transfer_id      TEXT PRIMARY KEY,
                source_stream_id TEXT    NOT NULL,
                target_stream_id TEXT    NOT NULL,
                amount           INTEGER NOT NULL CHECK (amount > 0),
                source_sequence  INTEGER NOT NULL CHECK (source_sequence >= 0),
                target_sequence  INTEGER NOT NULL CHECK (target_sequence >= 0),
                CHECK (source_stream_id <> target_stream_id),
                FOREIGN KEY (source_stream_id) REFERENCES streams(stream_id),
                FOREIGN KEY (target_stream_id) REFERENCES streams(stream_id)
            );

            -- One row per completed reversal: reversal_id is the global
            -- idempotency key, and (stream_id, event_id) is unique so each
            -- event can be reversed at most once. reversal_sequence records
            -- where the compensation event (event id
            -- `reversal:<reversal_id>`, delta = -original delta) was appended
            -- so an idempotent replay can return the original coordinates.
            CREATE TABLE IF NOT EXISTS reversals (
                reversal_id       TEXT PRIMARY KEY,
                stream_id         TEXT    NOT NULL,
                event_id          TEXT    NOT NULL,
                reversal_event_id TEXT    NOT NULL,
                reversal_sequence INTEGER NOT NULL CHECK (reversal_sequence >= 0),
                delta             INTEGER NOT NULL,
                UNIQUE (stream_id, event_id),
                FOREIGN KEY (stream_id, event_id)
                    REFERENCES events(stream_id, event_id)
            );

            -- One row per sealed stream: sealing is irreversible, so
            -- stream_id alone is the primary key. seal_id is the client-
            -- assigned idempotency key within the stream and
            -- next_sequence records the sealed version (the applied-prefix
            -- length at seal time). sha256 covers the canonical newline-
            -- joined JSON rendering of every event in sequence order.
            CREATE TABLE IF NOT EXISTS seals (
                stream_id     TEXT PRIMARY KEY,
                seal_id       TEXT    NOT NULL,
                next_sequence INTEGER NOT NULL CHECK (next_sequence >= 0),
                event_count   INTEGER NOT NULL CHECK (event_count >= 0),
                sha256        TEXT    NOT NULL,
                FOREIGN KEY (stream_id) REFERENCES streams(stream_id)
            );

            -- Defence in depth: a sealed stream may never gain another event.
            -- Application code rejects such attempts first; this trigger makes
            -- the invariant hold at the storage layer as well (it fires only on
            -- INSERT, so idempotent replays that write nothing are unaffected).
            CREATE TRIGGER IF NOT EXISTS trg_events_sealed_insert
            BEFORE INSERT ON events
            WHEN EXISTS (SELECT 1 FROM seals WHERE stream_id = NEW.stream_id)
            BEGIN
                SELECT RAISE(ABORT, 'stream is sealed and cannot accept new events');
            END;

            -- One row per (stream, consumer): the consumption cursor plus the
            -- at-most-one outstanding claim lease. Lease columns are either all
            -- NULL (no lease) or all set together. failure_seq/failure_count
            -- track the consecutive nacks of the batch head event at the
            -- cursor: they are NULL when there is no recorded failure (a fresh
            -- cursor, right after a successful ack, or after a dead-letter
            -- skip), and otherwise failure_seq equals cursor_seq.
            CREATE TABLE IF NOT EXISTS consumers (
                stream_id        TEXT    NOT NULL,
                consumer_id      TEXT    NOT NULL,
                cursor_seq       INTEGER NOT NULL DEFAULT 0 CHECK (cursor_seq >= 0),
                lease_token      TEXT,
                lease_expires_at INTEGER CHECK (lease_expires_at IS NULL OR lease_expires_at >= 0),
                lease_from_seq   INTEGER,
                lease_to_seq     INTEGER CHECK (lease_to_seq IS NULL OR lease_to_seq >= 0),
                failure_seq      INTEGER CHECK (failure_seq IS NULL OR failure_seq >= 0),
                failure_count    INTEGER CHECK (failure_count IS NULL OR failure_count >= 1),
                PRIMARY KEY (stream_id, consumer_id),
                FOREIGN KEY (stream_id) REFERENCES streams(stream_id),
                CHECK ((lease_token IS NULL) = (lease_expires_at IS NULL)),
                CHECK ((lease_token IS NULL) = (lease_from_seq IS NULL)),
                CHECK ((lease_token IS NULL) = (lease_to_seq IS NULL)),
                CHECK (lease_to_seq IS NULL OR lease_to_seq >= lease_from_seq),
                CHECK ((failure_seq IS NULL) = (failure_count IS NULL))
            );

            -- Idempotency ledger for nack requests: lease_token is the key.
            -- The first nack of a live lease writes its outcome here; replaying
            -- the same token (long after the lease itself was released) returns
            -- the stored original result and never counts again.
            CREATE TABLE IF NOT EXISTS consumer_nacks (
                stream_id     TEXT    NOT NULL,
                consumer_id   TEXT    NOT NULL,
                lease_token   TEXT    PRIMARY KEY,
                head_sequence INTEGER NOT NULL CHECK (head_sequence >= 0),
                attempt       INTEGER NOT NULL CHECK (attempt >= 1),
                dead_lettered INTEGER NOT NULL CHECK (dead_lettered IN (0, 1))
            );

            -- One row per (stream, consumer, sequence) dead-lettered after
            -- CONSUMER_MAX_ATTEMPTS consecutive nacks. Written in the same
            -- BEGIN IMMEDIATE transaction that advances the cursor past the
            -- event, so the event is never delivered again and the dead-letter
            -- record always exists.
            CREATE TABLE IF NOT EXISTS dead_letters (
                stream_id       TEXT    NOT NULL,
                consumer_id     TEXT    NOT NULL,
                sequence        INTEGER NOT NULL CHECK (sequence >= 0),
                event_id        TEXT    NOT NULL,
                delta           INTEGER NOT NULL,
                attempts        INTEGER NOT NULL CHECK (attempts >= 1),
                last_reason     TEXT    NOT NULL,
                quarantined_at  INTEGER NOT NULL CHECK (quarantined_at >= 0),
                PRIMARY KEY (stream_id, consumer_id, sequence),
                FOREIGN KEY (stream_id, consumer_id)
                    REFERENCES consumers(stream_id, consumer_id),
                FOREIGN KEY (stream_id, event_id)
                    REFERENCES events(stream_id, event_id)
            );

            CREATE INDEX IF NOT EXISTS idx_dead_letters_stream_consumer
                ON dead_letters (stream_id, consumer_id, sequence);
            """
        )
    finally:
        conn.close()


@contextmanager
def write_transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection and hold an immediate write transaction until commit/rollback."""
    conn = _connect()
    try:
        _apply_pragmas(conn)
        # Acquire the write lock up front so concurrent submitters serialise
        # cleanly instead of hitting SQLITE_BUSY mid-transaction.
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()


@contextmanager
def read_connection() -> Iterator[sqlite3.Connection]:
    """Open a short-lived connection for read-only queries."""
    conn = _connect()
    try:
        _apply_pragmas(conn)
        yield conn
    finally:
        conn.close()


@contextmanager
def read_transaction() -> Iterator[sqlite3.Connection]:
    """Open a connection and hold a read transaction until commit/rollback.

    In WAL mode a read transaction observes one consistent snapshot of the
    database for its whole lifetime and never blocks concurrent writers.
    """
    conn = _connect()
    try:
        _apply_pragmas(conn)
        conn.execute("BEGIN")
        yield conn
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    finally:
        conn.close()
