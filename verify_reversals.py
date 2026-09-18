"""Stdlib-only smoke test for event reversals (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "reversals.db")

from app import db, repo, consumers, forks, reversals  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# Seed a stream: applied events 0..2 with deltas [100, -30, 20] (balance 90).
for seq, delta in [(0, 100), (1, -30), (2, 20)]:
    repo.submit_event("acct", f"e{seq}", seq, delta)

# 1. happy path: compensation event appended at next_sequence and applied
r = reversals.create_reversal("acct", "e2", "rev-1")
check("reversal created", not r.replayed, str(r))
check(
    "reversal coordinates/state",
    r.reversal_event_id == "reversal:rev-1"
    and r.sequence == 3
    and r.delta == -20
    and r.state.balance == 70
    and r.state.next_sequence == 4
    and r.state.pending_sequences == [],
    str(r),
)
check("state visible", repo.get_stream_state("acct").balance == 70)

# 2. idempotent replay returns original coordinates + current state
repo.submit_event("acct", "e4", 4, 7)  # balance 77, next 5
r2 = reversals.create_reversal("acct", "e2", "rev-1")
check("replay flagged", r2.replayed)
check(
    "replay keeps coordinates, sees current state",
    r2.sequence == 3
    and r2.delta == -20
    and r2.state.balance == 77
    and r2.state.next_sequence == 5,
    str(r2),
)
check("no double compensation", repo.get_stream_state("acct").balance == 77)

# 3. same reversal_id, different target -> 409 reversal_replay_conflict
for stream_id, event_id in (("acct", "e1"), ("other", "o0")):
    try:
        reversals.create_reversal(stream_id, event_id, "rev-1")
        check(f"drift rejected {stream_id}/{event_id}", False)
    except reversals.ReversalError as e:
        check(
            f"drift -> reversal_replay_conflict ({e.code})",
            e.code == "reversal_replay_conflict" and e.status_code == 409,
        )

# 4. same event, new reversal_id -> 409 event_already_reversed
try:
    reversals.create_reversal("acct", "e2", "rev-other")
    check("second reversal rejected", False)
except reversals.ReversalError as e:
    check(
        "second reversal -> event_already_reversed",
        e.code == "event_already_reversed" and e.status_code == 409,
        str(e.code),
    )
# the failed reversal_id was not burned by the rejected attempt
repo.submit_event("other", "o0", 0, 9)
r3 = reversals.create_reversal("other", "o0", "rev-other")
check("failed id reusable", not r3.replayed and r3.state.balance == 0, str(r3))

# 5. missing stream/event -> 404, no side effects
try:
    reversals.create_reversal("ghost", "e0", "rev-404a")
    check("missing stream rejected", False)
except reversals.ReversalError as e:
    check(
        "missing stream -> 404",
        e.code == "stream_not_found" and e.status_code == 404,
        str((e.code, e.status_code)),
    )
try:
    reversals.create_reversal("acct", "ghost-event", "rev-404b")
    check("missing event rejected", False)
except reversals.ReversalError as e:
    check(
        "missing event -> 404",
        e.code == "event_not_found" and e.status_code == 404,
        str((e.code, e.status_code)),
    )
check("no phantom stream", repo.get_stream_state("ghost") is None)
check("stream untouched by 404", repo.get_stream_state("acct").balance == 77)

# 6. unapplied (buffered) event -> 409; applied after gap closes -> ok
repo.submit_event("pend", "p1", 1, 30)  # buffered, gap at 0
try:
    reversals.create_reversal("pend", "p1", "rev-pend")
    check("unapplied rejected", False)
except reversals.ReversalError as e:
    check(
        "unapplied -> event_not_applied",
        e.code == "event_not_applied" and e.status_code == 409,
        str((e.code, e.status_code)),
    )
repo.submit_event("pend", "p0", 0, 100)  # catch-up applies p0 and p1
rp = reversals.create_reversal("pend", "p1", "rev-pend")
check(
    "reversal after catch-up",
    rp.sequence == 2 and rp.state.balance == 100 and rp.state.next_sequence == 3,
    str(rp),
)

# 7. the compensation event closes a gap and unblocks buffered events
repo.submit_event("gap", "g0", 0, 10)
repo.submit_event("gap", "g2", 2, 5)  # buffered behind a gap at 1
rg = reversals.create_reversal("gap", "g0", "rev-gap")
check(
    "reversal unblocks buffered events",
    rg.sequence == 1
    and rg.state.balance == 5
    and rg.state.next_sequence == 3
    and rg.state.pending_sequences == [],
    str(rg),
)

# 8. reversal events are ordinary events: consumable and forkable
claim = consumers.claim("acct", "w", 100)
check(
    "reversal event consumable",
    [(ev.event_id, ev.delta) for ev in claim.events]
    == [("e0", 100), ("e1", -30), ("e2", 20), ("reversal:rev-1", -20), ("e4", 7)],
    str(claim),
)
check("ack reversal batch", consumers.ack("acct", "w", claim.lease_token) == 5)
f = forks.create_fork("acct", "fork-rev", "acct-fork", 4)
check("fork copies reversal", f.balance == 70 and f.next_sequence == 4, str(f))

# 9. reversing a reversal (undoing the undo)
rr = reversals.create_reversal("acct", "reversal:rev-1", "rev-undo")
check(
    "reversal of reversal",
    rr.delta == 20 and rr.state.balance == 97 and rr.state.next_sequence == 6,
    str(rr),
)

# 10. concurrent identical retries commit exactly once
repo.submit_event("c1", "c1e0", 0, 80)
results, errors = [], []


def do_retry():
    try:
        results.append(reversals.create_reversal("c1", "c1e0", "rev-race"))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_retry) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent retries no errors", not errors, str(errors[:1]))
check(
    "concurrent retries one commit",
    len(results) == 20
    and sum(1 for x in results if not x.replayed) == 1
    and sum(1 for x in results if x.replayed) == 19,
)
check("compensated once", repo.get_stream_state("c1").balance == 0)

# 11. concurrent distinct reversal_ids on one event: exactly one compensation
repo.submit_event("c2", "c2e0", 0, 80)
codes, wins, derrors = [], [], []


def do_distinct(n):
    try:
        reversals.create_reversal("c2", "c2e0", f"rev-many-{n}")
        wins.append(n)
    except reversals.ReversalError as e:
        codes.append(e.code)
    except Exception:
        derrors.append(traceback.format_exc())


threads = [threading.Thread(target=do_distinct, args=(n,)) for n in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("distinct reversals no unexpected errors", not derrors, str(derrors[:1]))
check("exactly one winner", len(wins) == 1, str(wins))
check(
    "losers all event_already_reversed",
    len(codes) == 19 and set(codes) == {"event_already_reversed"},
    str(set(codes)),
)
c2 = repo.get_stream_state("c2")
check(
    "single compensation",
    c2.balance == 0 and c2.next_sequence == 2,
    str(c2),
)

# 12. persistence: the ledger row survives
with db.read_connection() as conn:
    row = conn.execute(
        "SELECT stream_id, event_id, reversal_event_id, reversal_sequence, delta "
        "FROM reversals WHERE reversal_id = 'rev-1'"
    ).fetchone()
check(
    "ledger persisted",
    tuple(row) == ("acct", "e2", "reversal:rev-1", 3, -20),
    str(tuple(row) if row else None),
)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL REVERSAL CHECKS PASSED")
