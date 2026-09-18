"""Stdlib-only smoke test for snapshot export (run without FastAPI installed)."""

import base64
import hashlib
import hmac
import json
import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "snapshots.db")

from app import config, db, forks, repo, snapshots  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def walk(stream_id, limit, cursor=None):
    """Follow pages to the end; return (events, pages, last_page)."""
    events, pages, last = [], 0, None
    while True:
        page = snapshots.get_events_page(stream_id, limit, cursor)
        events.extend(page.events)
        pages += 1
        last = page
        if page.next_cursor is None:
            return events, pages, last
        cursor = page.next_cursor


def expect_cursor_error(name, fn, code):
    try:
        fn()
        check(name, False, "no error raised")
    except snapshots.CursorError as e:
        check(name, e.code == code, e.code)


# Seed a stream: applied events 0..4, deltas [100, -30, 20, 10, 5] (balance 105).
for seq, delta in [(0, 100), (1, -30), (2, 20), (3, 10), (4, 5)]:
    repo.submit_event("acct", f"e{seq}", seq, delta)

# 1. first page pins the snapshot: bound, version balance, first events
p1 = snapshots.get_events_page("acct", 2, None)
check("first page bound", p1.snapshot_next_sequence == 5, str(p1))
check("first page balance", p1.balance == 105, str(p1.balance))
check(
    "first page events",
    [(e.event_id, e.sequence) for e in p1.events] == [("e0", 0), ("e1", 1)],
    str(p1.events),
)
check("first page has cursor", p1.next_cursor is not None)

# 2. concurrent appends after the snapshot was pinned
repo.submit_event("acct", "e5", 5, 60)
repo.submit_event("acct", "e6", 6, 70)  # live state: balance 235, next 7

# 3. the ongoing export still reads the pinned snapshot: no leak/skip/dup
rest, pages, last = walk("acct", 2, p1.next_cursor)
all_events = list(p1.events) + rest
check(
    "snapshot complete and exact",
    [e.sequence for e in all_events] == [0, 1, 2, 3, 4],
    str([e.sequence for e in all_events]),
)
check("snapshot page count", pages == 2, str(pages))
check("last page ends export", last.next_cursor is None)
check("snapshot balance frozen", last.balance == 105, str(last.balance))
check("snapshot bound frozen", last.snapshot_next_sequence == 5)

# 4. a fresh export pins a new snapshot including the appended events
fresh, _, fresh_last = walk("acct", 100)
check(
    "fresh snapshot sees appends",
    [e.sequence for e in fresh] == list(range(7))
    and fresh_last.snapshot_next_sequence == 7
    and fresh_last.balance == 235,
    str(fresh_last),
)

# 5. reusing a cursor replays the identical page (idempotent pagination)
r1 = snapshots.get_events_page("acct", 2, p1.next_cursor)
r2 = snapshots.get_events_page("acct", 2, p1.next_cursor)
check("cursor replay deterministic", r1 == r2)

# 6. cursor shape: b64url(canonical JSON) + '.' + b64url(HMAC-SHA256)
body_b64, sig_b64 = p1.next_cursor.split(".")
payload = base64.urlsafe_b64decode(body_b64)
check(
    "cursor payload is canonical JSON",
    payload == b'{"next_sequence":2,"snapshot_next_sequence":5,"stream_id":"acct"}',
    payload.decode(),
)
expected_sig = hmac.new(
    config.snapshot_cursor_secret().encode("utf-8"), payload, hashlib.sha256
).digest()
check("cursor signature verifies", base64.urlsafe_b64decode(sig_b64) == expected_sig)
check(
    "cursor round-trips",
    snapshots.decode_cursor(p1.next_cursor) == ("acct", 5, 2),
)
check(
    "cursor encoding deterministic",
    snapshots.encode_cursor("acct", 5, 2) == p1.next_cursor,
)

# 7. pending (buffered) events are not exported
repo.submit_event("gap", "g1", 1, 9)  # buffered behind the gap at 0
g = snapshots.get_events_page("gap", 10, None)
check(
    "pending excluded",
    g.snapshot_next_sequence == 0 and g.events == [] and g.balance == 0
    and g.next_cursor is None,
    str(g),
)
repo.submit_event("gap", "g0", 0, 4)  # catch-up applies 0 and 1
g2 = snapshots.get_events_page("gap", 10, None)
check(
    "after catch-up exported",
    g2.snapshot_next_sequence == 2 and g2.balance == 13
    and [e.sequence for e in g2.events] == [0, 1],
    str(g2),
)

# 8. empty stream (forked at through_sequence 0): empty single page
forks.create_fork("acct", "ef", "empty", 0)
e = snapshots.get_events_page("empty", 10, None)
check(
    "empty stream page",
    e.snapshot_next_sequence == 0 and e.events == [] and e.balance == 0
    and e.next_cursor is None,
    str(e),
)

# 9. unknown stream -> None (HTTP 404), with or without a cursor
check("unknown stream -> None", snapshots.get_events_page("ghost", 10, None) is None)
check(
    "unknown stream with cursor -> None",
    snapshots.get_events_page("ghost", 10, p1.next_cursor) is None,
)

# 10. tampered cursors -> invalid_cursor
def tampered_payload(cursor):
    body, sig = cursor.split(".")
    return ("A" if body[0] != "A" else "B") + body[1:] + "." + sig


def tampered_signature(cursor):
    body, sig = cursor.split(".")
    return body + "." + ("A" if sig[0] != "A" else "B") + sig[1:]


expect_cursor_error(
    "tampered payload -> invalid_cursor",
    lambda: snapshots.get_events_page("acct", 2, tampered_payload(p1.next_cursor)),
    "invalid_cursor",
)
expect_cursor_error(
    "tampered signature -> invalid_cursor",
    lambda: snapshots.get_events_page("acct", 2, tampered_signature(p1.next_cursor)),
    "invalid_cursor",
)
for bad in ("not-a-cursor", "!!!.@@@", "", p1.next_cursor + ".extra", "."):
    expect_cursor_error(
        f"malformed cursor {bad!r} -> invalid_cursor",
        lambda bad=bad: snapshots.get_events_page("acct", 2, bad),
        "invalid_cursor",
    )

# 11. cross-stream use -> cursor_stream_mismatch
repo.submit_event("other", "o0", 0, 1)
expect_cursor_error(
    "cross-stream cursor -> cursor_stream_mismatch",
    lambda: snapshots.get_events_page("other", 2, p1.next_cursor),
    "cursor_stream_mismatch",
)

# 12. validly signed but field-invalid payloads -> invalid_cursor
def craft(payload_dict):
    raw = json.dumps(
        payload_dict, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    body = base64.urlsafe_b64encode(raw).decode("ascii")
    sig = base64.urlsafe_b64encode(
        hmac.new(config.snapshot_cursor_secret().encode("utf-8"), raw, hashlib.sha256).digest()
    ).decode("ascii")
    return f"{body}.{sig}"


bad_payloads = [
    {"stream_id": "acct"},  # missing fields
    {"stream_id": "acct", "snapshot_next_sequence": 5, "next_sequence": 2, "x": 1},
    {"stream_id": "acct", "snapshot_next_sequence": 5, "next_sequence": 6},  # past bound
    {"stream_id": "acct", "snapshot_next_sequence": -1, "next_sequence": 0},
    {"stream_id": "acct", "snapshot_next_sequence": 5, "next_sequence": -1},
    {"stream_id": "acct", "snapshot_next_sequence": 5, "next_sequence": True},
    {"stream_id": "acct", "snapshot_next_sequence": "5", "next_sequence": 2},
    {"stream_id": "", "snapshot_next_sequence": 5, "next_sequence": 2},
]
for payload_dict in bad_payloads:
    expect_cursor_error(
        f"invalid fields {payload_dict} -> invalid_cursor",
        lambda c=craft(payload_dict): snapshots.get_events_page("acct", 2, c),
        "invalid_cursor",
    )

# 13. unicode stream id survives the cursor round-trip
repo.submit_event("流-快照", "u0", 0, 7)
repo.submit_event("流-快照", "u1", 1, 8)
up1 = snapshots.get_events_page("流-快照", 1, None)
up2 = snapshots.get_events_page("流-快照", 1, up1.next_cursor)
check(
    "unicode stream id round-trip",
    [e.sequence for e in up2.events] == [1] and up2.balance == 15
    and up2.next_cursor is None,
    str(up2),
)

# 14. concurrent first-page requests each get a self-consistent snapshot
for seq in range(10):
    repo.submit_event("conc", f"c{seq}", seq, 1)  # deltas all 1 -> balance == next_sequence
results, errors = [], []


def first_page():
    try:
        page = snapshots.get_events_page("conc", 100, None)
        results.append(page)
    except Exception:
        errors.append(traceback.format_exc())


def appender():
    try:
        for seq in range(10, 20):
            repo.submit_event("conc", f"c{seq}", seq, 1)
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=first_page) for _ in range(20)]
threads.append(threading.Thread(target=appender))
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent snapshots no errors", not errors, str(errors[:1]))
check(
    "every snapshot self-consistent",
    all(
        p.balance == p.snapshot_next_sequence
        and len(p.events) == p.snapshot_next_sequence
        and [e.sequence for e in p.events] == list(range(p.snapshot_next_sequence))
        for p in results
    ),
    str([(p.snapshot_next_sequence, p.balance, len(p.events)) for p in results]),
)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL SNAPSHOT CHECKS PASSED")
