"""Stdlib-only smoke test for cross-stream transfers (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "transfers.db")

from app import db, repo, consumers, transfers  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# Two funded streams and one empty one.
repo.submit_event("a", "a0", 0, 100)
repo.submit_event("b", "b0", 0, 0)
repo.submit_event("c", "c0", 0, 5)

# 1. happy path: debit + credit, both events applied
t = transfers.create_transfer("tx1", "a", "b", 30)
check("transfer created", not t.replayed, str(t))
check(
    "transfer coordinates/states",
    t.source_sequence == 1
    and t.target_sequence == 1
    and t.source.balance == 70
    and t.source.next_sequence == 2
    and t.target.balance == 30
    and t.target.next_sequence == 2
    and t.source_event_id == "transfer:tx1:debit"
    and t.target_event_id == "transfer:tx1:credit",
    str(t),
)
check(
    "money conserved",
    repo.get_stream_state("a").balance + repo.get_stream_state("b").balance == 100,
)

# 2. idempotent replay returns original coordinates and does not debit twice
t2 = transfers.create_transfer("tx1", "a", "b", 30)
check("replay flagged", t2.replayed)
check(
    "replay identical coordinates",
    t2.source_sequence == 1 and t2.target_sequence == 1,
    str(t2),
)
check("no double debit", repo.get_stream_state("a").balance == 70)

# 3. replay reports the *current* state after later events
repo.submit_event("a", "a2", 2, -5)
t3 = transfers.create_transfer("tx1", "a", "b", 30)
check(
    "replay sees current state",
    t3.source.balance == 65 and t3.source.next_sequence == 3 and t3.source_sequence == 1,
    str(t3),
)

# 4. parameter drift -> 409 transfer_replay_conflict
for kwargs in (
    dict(transfer_id="tx1", source_stream_id="a", target_stream_id="b", amount=31),
    dict(transfer_id="tx1", source_stream_id="c", target_stream_id="b", amount=30),
    dict(transfer_id="tx1", source_stream_id="a", target_stream_id="c", amount=30),
):
    try:
        transfers.create_transfer(**kwargs)
        check(f"drift rejected {kwargs['target_stream_id']}", False)
    except transfers.TransferError as e:
        check(f"drift -> transfer_replay_conflict ({e.code})", e.code == "transfer_replay_conflict")

# 5. insufficient funds -> 422, nothing stored
try:
    transfers.create_transfer("tx-poor", "c", "b", 6)
    check("insufficient funds rejected", False)
except transfers.TransferError as e:
    check(
        "insufficient funds -> 422",
        e.code == "insufficient_funds" and e.status_code == 422,
        str((e.code, e.status_code)),
    )
# the failed transfer_id can still be used, and balances are untouched
transfers.create_transfer("tx-poor", "c", "b", 5)
check("after failed attempt", repo.get_stream_state("c").balance == 0
      and repo.get_stream_state("b").balance == 35)

# 6. missing streams -> 404, no side effects
for src, dst, code in (
    ("ghost", "b", "source_stream_not_found"),
    ("a", "ghost", "target_stream_not_found"),
):
    try:
        transfers.create_transfer(f"tx-404-{code}", src, dst, 1)
        check(f"missing {code} rejected", False)
    except transfers.TransferError as e:
        check(f"missing -> {code} 404", e.code == code and e.status_code == 404)
check("no phantom stream", repo.get_stream_state("ghost") is None)
check("source untouched by 404", repo.get_stream_state("a").balance == 65)

# 7. exact balance is allowed and reaches zero
repo.submit_event("d", "d0", 0, 7)
te = transfers.create_transfer("tx-zero", "d", "b", 7)
check("exact balance transfer", te.source.balance == 0 and te.source.next_sequence == 2)

# 8. parked buffered events get applied by the contiguous catch-up
repo.submit_event("e", "e1", 1, 4)  # buffered, gap at 0
repo.submit_event("f", "f1", 1, 9)  # buffered, gap at 0
repo.submit_event("e", "e0", 0, 10)  # -> balance 14, next 2
repo.submit_event("f", "f0", 0, 0)  # -> balance 9, next 2
tg = transfers.create_transfer("tx-gap", "e", "f", 4)
check(
    "transfer through streams with no pending gap",
    tg.source.balance == 10 and tg.target.balance == 13
    and tg.source.next_sequence == 3 and tg.target.next_sequence == 3,
    str(tg),
)

# transfer events are consumable through the existing claim/ack API
claim = consumers.claim("e", "w", 100)
check(
    "debit event consumable",
    [(ev.sequence, ev.delta) for ev in claim.events] == [(0, 10), (1, 4), (2, -4)],
    str(claim),
)
check("ack debit batch", consumers.ack("e", "w", claim.lease_token) == 3)

# 9. concurrent identical retries commit exactly once
repo.submit_event("g", "g0", 0, 100)
repo.submit_event("h", "h0", 0, 0)
results = []
errors = []


def do_retry():
    try:
        results.append(transfers.create_transfer("tx-race", "g", "h", 40))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_retry) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent retries no errors", not errors, str(errors[:1]))
check("concurrent retries one commit", len(results) == 20
      and sum(1 for r in results if not r.replayed) == 1
      and sum(1 for r in results if r.replayed) == 19)
check(
    "concurrent balances",
    repo.get_stream_state("g").balance == 60 and repo.get_stream_state("h").balance == 40,
)

# 10. many distinct concurrent transfers serialise and conserve money
repo.submit_event("i", "i0", 0, 100)
repo.submit_event("j", "j0", 0, 100)
derrors = []


def do_distinct(n):
    try:
        transfers.create_transfer(f"tx-many-{n}", "i", "j", 1)
    except Exception:
        derrors.append(traceback.format_exc())


threads = [threading.Thread(target=do_distinct, args=(n,)) for n in range(50)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("distinct transfers no errors", not derrors, str(derrors[:1]))
si, sj = repo.get_stream_state("i"), repo.get_stream_state("j")
check(
    "distinct transfers landed once each",
    si.balance == 50 and si.next_sequence == 51 and sj.balance == 150
    and sj.next_sequence == 51 and si.balance + sj.balance == 200,
    str((si.balance, sj.balance)),
)

# 11. persistence: reopen and the ledger row is still there
with db.read_connection() as conn:
    row = conn.execute(
        "SELECT source_sequence, target_sequence, amount FROM transfers WHERE transfer_id = 'tx1'"
    ).fetchone()
check("ledger persisted", tuple(row) == (1, 1, 30), str(tuple(row) if row else None))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL TRANSFER CHECKS PASSED")
