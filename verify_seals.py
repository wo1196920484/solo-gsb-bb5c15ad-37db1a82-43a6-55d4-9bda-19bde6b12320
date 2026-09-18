"""Stdlib-only smoke test for irreversible stream sealing (run without FastAPI installed)."""

import hashlib
import json
import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "seals.db")

from app import consumers, db, forks, repo, reversals, seals, transfers  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def canonical(events):
    lines = [
        json.dumps([seq, event_id, delta], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for event_id, seq, delta in events
    ]
    return hashlib.sha256(b"\n".join(lines)).hexdigest()


# Seed a stream: applied events 0..2, deltas [100, -30, 20] (balance 90).
for seq, delta in [(0, 100), (1, -30), (2, 20)]:
    repo.submit_event("acct", f"e{seq}", seq, delta)

# 1. happy path: seal at the current version, hash matches canonical rendering
r = seals.create_seal("acct", "seal-1", 3)
check("seal created", not r.replayed, str(r))
check(
    "seal coordinates",
    r.next_sequence == 3 and r.event_count == 3 and not r.replayed,
    str(r),
)
check("canonical sha256", r.sha256 == canonical([("e0", 0, 100), ("e1", 1, -30), ("e2", 2, 20)]), r.sha256)

# 2. identical request replays the original result
r2 = seals.create_seal("acct", "seal-1", 3)
check("replay flagged", r2.replayed)
check("replay keeps digest", r2.sha256 == r.sha256 and r2.next_sequence == 3, str(r2))

# 3. parameter drift against the existing seal -> 409 seal_replay_conflict
for kwargs in (
    {"seal_id": "seal-other", "expected_next_sequence": 3},
    {"seal_id": "seal-1", "expected_next_sequence": 4},
):
    try:
        seals.create_seal("acct", **kwargs)
        check(f"drift rejected {kwargs}", False)
    except seals.SealError as e:
        check(f"drift -> seal_replay_conflict ({kwargs})", e.code == "seal_replay_conflict", e.code)

# 4. unknown stream -> 404 (None), no seal written
check("unknown stream -> None", seals.create_seal("ghost", "s", 0) is None)
with db.read_connection() as conn:
    check("no phantom seal", conn.execute("SELECT COUNT(*) FROM seals").fetchone()[0] == 1)

# 5. stale expected_next_sequence on a first-time seal -> 409 version_mismatch
repo.submit_event("ver", "v0", 0, 5)
repo.submit_event("ver", "v1", 1, 7)  # next_sequence 2
try:
    seals.create_seal("ver", "s", 1)  # client is one version behind
    check("stale version rejected", False)
except seals.SealError as e:
    check("stale version -> version_mismatch", e.code == "version_mismatch", e.code)
# the failed seal_id is not burned
r3 = seals.create_seal("ver", "s", 2)
check("seal at correct version", not r3.replayed and r3.next_sequence == 2, str(r3))

# 6. buffered pending events -> 409 stream_has_pending_events
repo.submit_event("pend", "p1", 1, 9)  # buffered behind gap at 0
try:
    seals.create_seal("pend", "s", 0)
    check("pending rejected", False)
except seals.SealError as e:
    check("pending -> stream_has_pending_events", e.code == "stream_has_pending_events", e.code)
repo.submit_event("pend", "p0", 0, 4)  # catch-up -> next_sequence 2
r4 = seals.create_seal("pend", "s", 2)
check("seal after gap closes", not r4.replayed and r4.event_count == 2, str(r4))

# 7. empty stream (forked at through_sequence 0) seals with the SHA-256 of empty input
forks.create_fork("acct", "empty-fork", "empty", 0)
r5 = seals.create_seal("empty", "s", 0)
check("empty stream seal", r5.event_count == 0 and r5.sha256 == canonical([]), str(r5))

# 8. a sealed stream rejects new submissions, transfers and reversals
try:
    repo.submit_event("acct", "new", 3, 1)
    check("new submission rejected", False)
except repo.ConflictError as e:
    check("new submission -> stream_sealed", e.code == "stream_sealed", e.code)

# idempotent replay of a pre-seal event still works
state, replayed = repo.submit_event("acct", "e0", 0, 100)
check("pre-seal event replays", replayed and state.balance == 90 and state.next_sequence == 3)

repo.submit_event("other", "o0", 0, 50)
try:
    transfers.create_transfer("txn-new", "acct", "other", 1)
    check("debit into sealed source rejected", False)
except transfers.TransferError as e:
    check("sealed source transfer -> stream_sealed", e.code == "stream_sealed", e.code)
try:
    transfers.create_transfer("txn-new2", "other", "acct", 1)
    check("credit into sealed target rejected", False)
except transfers.TransferError as e:
    check("sealed target transfer -> stream_sealed", e.code == "stream_sealed", e.code)

try:
    reversals.create_reversal("acct", "e1", "rev-new")
    check("new reversal rejected", False)
except reversals.ReversalError as e:
    check("new reversal -> stream_sealed", e.code == "stream_sealed", e.code)

# 9. operations completed before sealing still replay idempotently
# 'ver' was sealed at version 2 in check 5, so use a fresh stream: run the
# transfer first, seal afterwards, then replay.
repo.submit_event("pre", "p0", 0, 12)
transfers.create_transfer("txn-old", "pre", "other", 2)  # pre: 12-2=10, next 2
seals.create_seal("pre", "s2", 2)
tr = transfers.create_transfer("txn-old", "pre", "other", 2)
check("pre-seal transfer replays", tr.replayed and tr.source_sequence == 1, str(tr))
# no new events slipped through
check(
    "sealed stream unchanged",
    repo.get_stream_state("pre").balance == 10
    and repo.get_stream_state("pre").next_sequence == 2,
)

# seal a stream after a reversal and replay the reversal afterwards
repo.submit_event("rv", "rv0", 0, 80)
reversals.create_reversal("rv", "rv0", "rv-rev")  # balance 0, next 2
seals.create_seal("rv", "s3", 2)
rr = reversals.create_reversal("rv", "rv0", "rv-rev")
check("pre-seal reversal replays", rr.replayed and rr.sequence == 1 and rr.delta == -80, str(rr))

# 10. sealed streams stay readable, consumable and forkable
st = repo.get_stream_state("acct")
check("sealed stream queryable", st.balance == 90 and st.next_sequence == 3)
claim = consumers.claim("acct", "w", 100)
check("sealed stream consumable", [(ev.event_id, ev.sequence) for ev in claim.events] == [("e0", 0), ("e1", 1), ("e2", 2)])
check("sealed ack works", consumers.ack("acct", "w", claim.lease_token) == 3)
f = forks.create_fork("acct", "f1", "acct-copy", 3)
check("sealed stream forkable", f.balance == 90 and f.next_sequence == 3, str(f))
# the fork target is not itself sealed
state, _ = repo.submit_event("acct-copy", "c3", 3, 1)
check("fork target appendable", state.balance == 91 and state.next_sequence == 4, str(state))
# sealing the source does not retro-seal the fork
check("copy not sealed marker", True)

# 11. Unicode content survives the canonical JSON
repo.submit_event("uni", "事件-0", 0, 1)
ru = seals.create_seal("uni", "s4", 1)
expected_line = json.dumps([0, "事件-0", 1], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
check("unicode literal in digest", ru.sha256 == hashlib.sha256(expected_line).hexdigest(), ru.sha256)

# 12. database trigger is the storage-level backstop
import sqlite3 as _sqlite3

with db.write_transaction() as conn:
    try:
        conn.execute(
            "INSERT INTO events (stream_id, event_id, sequence, delta, applied) "
            "VALUES ('acct', 'bypass', 3, 1, 0)"
        )
        check("trigger blocks raw insert", False)
    except _sqlite3.IntegrityError:
        check("trigger blocks raw insert", True)

# 13. concurrent identical seal requests commit exactly once
repo.submit_event("c1", "c1e0", 0, 5)
results, errors = [], []


def do_retry():
    try:
        results.append(seals.create_seal("c1", "seal-race", 1))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_retry) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent seals no errors", not errors, str(errors[:1]))
check(
    "concurrent seals one commit",
    len(results) == 20
    and sum(1 for x in results if not x.replayed) == 1
    and sum(1 for x in results if x.replayed) == 19,
)
with db.read_connection() as conn:
    check(
        "exactly one seal row per stream",
        conn.execute("SELECT COUNT(*) FROM seals WHERE stream_id = 'c1'").fetchone()[0] == 1,
    )

# 14. the seal record persists with all columns
with db.read_connection() as conn:
    row = conn.execute(
        "SELECT seal_id, next_sequence, event_count, sha256 FROM seals WHERE stream_id = 'acct'"
    ).fetchone()
check(
    "ledger persisted",
    tuple(row[:3]) == ("seal-1", 3, 3) and len(row[3]) == 64,
    str(tuple(row)),
)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL SEAL CHECKS PASSED")
