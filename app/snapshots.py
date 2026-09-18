"""Event history snapshot export with HMAC-signed pagination cursors.

The first ``GET /streams/{stream_id}/events`` request (no ``cursor``) pins the
stream's current ``next_sequence`` as the snapshot upper bound inside a single
read transaction and returns the first page of *applied* events in
``[0, snapshot_next_sequence)`` together with the balance at that version.
Follow-up requests pass the opaque ``next_cursor`` from the previous page.

The cursor is ``"<b64url(payload)>.<b64url(signature)>"`` where the payload is
the canonical JSON rendering of ``{"stream_id", "snapshot_next_sequence",
"next_sequence"}`` (sorted keys, compact separators, UTF-8, non-ASCII kept
literal) and the signature is HMAC-SHA256 keyed with
``SNAPSHOT_CURSOR_SECRET``. The binding makes every page read the same
immutable snapshot: events appended concurrently can never leak into an
ongoing export, and no applied event is skipped or delivered twice. Applied
events below the pinned bound never change, so re-reading them page by page
stays consistent with the original snapshot; the per-page balance is the sum
of the deltas below the bound, which is exactly the balance the stream had
when the snapshot was pinned.

A corrupted signature, a cursor issued for another stream, or invalid payload
fields are rejected with HTTP 400; the last page carries
``next_cursor = null``.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Any

from . import config, db


class CursorError(Exception):
    """Raised when a cursor is malformed, forged or foreign (HTTP 400)."""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code
        self.message = message


@dataclass(frozen=True)
class SnapshotEvent:
    event_id: str
    sequence: int
    delta: int

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "sequence": self.sequence,
            "delta": self.delta,
        }


@dataclass(frozen=True)
class SnapshotPage:
    stream_id: str
    snapshot_next_sequence: int  # pinned upper bound (exclusive)
    balance: int  # balance at the snapshot version
    events: list[SnapshotEvent]
    next_cursor: str | None  # None on the last page

    def as_dict(self) -> dict[str, Any]:
        return {
            "stream_id": self.stream_id,
            "snapshot_next_sequence": self.snapshot_next_sequence,
            "balance": self.balance,
            "events": [event.as_dict() for event in self.events],
            "next_cursor": self.next_cursor,
        }


def _canonical_payload(
    stream_id: str, snapshot_next_sequence: int, next_sequence: int
) -> bytes:
    """Render the cursor fields as canonical JSON.

    ``sort_keys=True`` fixes the key order, the compact separators guarantee
    no insignificant whitespace and ``ensure_ascii=False`` keeps non-ASCII
    characters literal — the same conventions as the seal digest.
    """
    return json.dumps(
        {
            "stream_id": stream_id,
            "snapshot_next_sequence": snapshot_next_sequence,
            "next_sequence": next_sequence,
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _signature(payload: bytes) -> bytes:
    return hmac.new(
        config.snapshot_cursor_secret().encode("utf-8"), payload, hashlib.sha256
    ).digest()


def encode_cursor(
    stream_id: str, snapshot_next_sequence: int, next_sequence: int
) -> str:
    """Build the signed cursor pointing at ``next_sequence`` within the snapshot."""
    payload = _canonical_payload(stream_id, snapshot_next_sequence, next_sequence)
    body = base64.urlsafe_b64encode(payload).decode("ascii")
    sig = base64.urlsafe_b64encode(_signature(payload)).decode("ascii")
    return f"{body}.{sig}"


def decode_cursor(cursor: str) -> tuple[str, int, int]:
    """Verify and unpack a cursor.

    Returns ``(stream_id, snapshot_next_sequence, next_sequence)``. Raises
    :class:`CursorError` (HTTP 400) for malformed input, a signature mismatch
    or invalid payload fields.
    """
    parts = cursor.split(".")
    if len(parts) != 2 or not all(parts):
        raise CursorError(
            "invalid_cursor", "cursor must have the form '<payload>.<signature>'"
        )
    try:
        payload = base64.b64decode(parts[0], altchars=b"-_", validate=True)
        signature = base64.b64decode(parts[1], altchars=b"-_", validate=True)
    except (binascii.Error, ValueError):
        raise CursorError(
            "invalid_cursor", "cursor is not valid URL-safe base64"
        ) from None
    if not hmac.compare_digest(signature, _signature(payload)):
        raise CursorError("invalid_cursor", "cursor signature mismatch")
    try:
        data = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise CursorError(
            "invalid_cursor", "cursor payload is not valid JSON"
        ) from None
    if not isinstance(data, dict) or set(data) != {
        "stream_id",
        "snapshot_next_sequence",
        "next_sequence",
    }:
        raise CursorError(
            "invalid_cursor",
            "cursor payload must contain exactly stream_id, "
            "snapshot_next_sequence and next_sequence",
        )
    stream_id = data["stream_id"]
    snapshot_next = data["snapshot_next_sequence"]
    next_sequence = data["next_sequence"]
    if not isinstance(stream_id, str) or not stream_id:
        raise CursorError(
            "invalid_cursor", "cursor stream_id must be a non-empty string"
        )
    for value in (snapshot_next, next_sequence):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise CursorError(
                "invalid_cursor",
                "cursor sequence fields must be non-negative integers",
            )
    if next_sequence > snapshot_next:
        raise CursorError(
            "invalid_cursor", "cursor next_sequence exceeds snapshot_next_sequence"
        )
    return stream_id, snapshot_next, next_sequence


def get_events_page(
    stream_id: str, limit: int, cursor: str | None
) -> SnapshotPage | None:
    """Return one page of the applied-event snapshot for ``stream_id``.

    ``cursor=None`` pins a fresh snapshot at the stream's current
    ``next_sequence`` inside one read transaction; otherwise the snapshot
    bound and the page position come from the verified cursor. Returns
    ``None`` when the stream does not exist (HTTP 404). Raises
    :class:`CursorError` (HTTP 400) for a malformed or foreign cursor.
    """
    with db.read_transaction() as conn:
        stream = conn.execute(
            "SELECT stream_id, next_sequence FROM streams WHERE stream_id = ?",
            (stream_id,),
        ).fetchone()
        if stream is None:
            return None

        if cursor is None:
            # Pin the snapshot: the bound, the balance base and the first page
            # are all read inside this one transaction, so they are mutually
            # consistent even while other requests keep appending.
            snapshot_next = stream["next_sequence"]
            start = 0
        else:
            cursor_stream, snapshot_next, start = decode_cursor(cursor)
            if cursor_stream != stream_id:
                raise CursorError(
                    "cursor_stream_mismatch",
                    f"cursor was issued for stream {cursor_stream!r}, "
                    f"not {stream_id!r}",
                )

        rows = conn.execute(
            "SELECT event_id, sequence, delta FROM events "
            "WHERE stream_id = ? AND applied = 1 AND sequence >= ? AND sequence < ? "
            "ORDER BY sequence ASC LIMIT ?",
            (stream_id, start, snapshot_next, limit),
        ).fetchall()

        # Applied events in [0, snapshot_next) are immutable, so this sum is
        # the balance at the snapshot version on every page — no matter how
        # many events have been appended since the snapshot was pinned.
        balance = conn.execute(
            "SELECT COALESCE(SUM(delta), 0) AS balance FROM events "
            "WHERE stream_id = ? AND applied = 1 AND sequence < ?",
            (stream_id, snapshot_next),
        ).fetchone()["balance"]

    events = [SnapshotEvent(row["event_id"], row["sequence"], row["delta"]) for row in rows]
    next_start = start + len(events)
    next_cursor = (
        encode_cursor(stream_id, snapshot_next, next_start)
        if next_start < snapshot_next
        else None
    )
    return SnapshotPage(
        stream_id=stream_id,
        snapshot_next_sequence=snapshot_next,
        balance=balance,
        events=events,
        next_cursor=next_cursor,
    )
