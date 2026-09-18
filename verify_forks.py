"""Stdlib-only smoke test for stream forking (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "forks.db")

from app import db, repo, consumers, forks  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# Source stream: applied 0..4 with deltas [100, -30, 20, 10, 5] (balance 105),
# plus buffered pending events 6 and 7 (gap at 5 keeps them unapplied).
for seq, delta in [(0, 100), (1, -30), (2, 20), (3, 10), (4, 5)]:
    repo.submit_event("acct", f"e{seq}", seq, delta)
repo.submit_event("acct", "e6", 6, 60)
repo.submit_event("acct", "e7", 7, 70)
src = repo.get_stream_state("acct")
check("source baseline", src.balance == 105 and src.next_sequence == 5
      and src.pending_sequences == [6, 7], str(src))

# 1. full-prefix fork
f = forks.create_fork("acct", "f-full", "acct-fork", 5)
check("fork created", f is not None and not f.replayed, str(f))
check("fork response fields", f.source_stream_id == "acct"
      and f.target_stream_id == "acct-fork" and f.through_sequence == 5
      and f.balance == 105 and f.next_sequence == 5, str(f))
tgt = repo.get_stream_state("acct-fork")
check("target state", tgt.balance == 105 and tgt.next_sequence == 5
      and tgt.pending_sequences == [], str(tgt))

# copied events are applied and identical; pending events were not copied
with db.read_connection() as rconn:
    rows = rconn.execute(
        "SELECT sequence, delta, applied FROM events WHERE stream_id = 'acct-fork' ORDER BY sequence"
    ).fetchall()
check("copied events applied verbatim", [tuple(r) for r in rows]
      == [(0, 100, 1), (1, -30, 1), (2, 20, 1), (3, 10, 1), (4, 5, 1)],
      str([tuple(r) for r in rows]))

# 2. target is appendable: submit at next_sequence, continues balance
s, rep = repo.submit_event("acct-fork", "new-e5", 5, 7)
check("append to fork continues", not rep and s.next_sequence == 6
      and s.balance == 112 and s.pending_sequences == [], str(s))

# 3. partial cut
f2 = forks.create_fork("acct", "f-part", "acct-part", 2)
tgt2 = repo.get_stream_state("acct-part")
check("partial fork", f2.balance == 70 and f2.next_sequence == 2
      and tgt2.balance == 70 and tgt2.next_sequence == 2
      and tgt2.pending_sequences == [], str((f2, tgt2)))

# 4. zero cut
f0 = forks.create_fork("acct", "f-zero", "acct-zero", 0)
tgt0 = repo.get_stream_state("acct-zero")
check("zero fork", f0.balance == 0 and f0.next_sequence == 0
      and tgt0.balance == 0 and tgt0.next_sequence == 0, str(f0))

# 5. out of range -> ForkError 422 (both sides)
for bad in (-1, 6):
    try:
        forks.create_fork("acct", f"f-bad-{bad}", f"acct-bad-{bad}", bad)
        check(f"range reject {bad}", False)
    except forks.ForkError as e:
        # -1 is blocked by pydantic ge=0 at the API layer; repo guards 0..next_seq
        if bad < 0:
            check(f"range reject {bad}", e.code == "through_sequence_out_of_range")
        else:
            check(f"range reject {bad}",
                  e.code == "through_sequence_out_of_range" and e.status_code == 422,
                  str((e.code, e.status_code)))

# 6. source missing -> None
check("missing source None", forks.create_fork("nope", "x", "nope-t", 0) is None)

# 7. target already exists -> 409
try:
    forks.create_fork("acct", "f-dup-target", "acct-fork", 3)
    check("target exists 409", False)
except forks.ForkError as e:
    check("target exists 409", e.code == "target_stream_exists"
          and e.status_code == 409, str(e.code))

# 8. idempotent replay: same fork_id same params -> existing fork, replayed
fr = forks.create_fork("acct", "f-full", "acct-fork", 5)
check("replay existing fork", fr.replayed and fr.balance == 105
      and fr.next_sequence == 5 and fr.through_sequence == 5, str(fr))

# 8b. same fork_id, different through_sequence -> 409
try:
    forks.create_fork("acct", "f-full", "acct-fork", 3)
    check("replay diff cut 409", False)
except forks.ForkError as e:
    check("replay diff cut 409", e.code == "fork_replay_conflict"
          and e.status_code == 409, str(e.code))

# 8c. same fork_id, different target -> 409
try:
    forks.create_fork("acct", "f-full", "acct-other", 5)
    check("replay diff target 409", False)
except forks.ForkError as e:
    check("replay diff target 409", e.code == "fork_replay_conflict", str(e.code))

# 9. consumers can claim on the forked stream (uses copied applied events)
c = consumers.claim("acct-part", "w", 10)
check("claim on fork", [e.sequence for e in c.events] == [0, 1]
      and c.to_sequence == 2, str(c))

# 10. fork of a fork
ff = forks.create_fork("acct-fork", "f-of-f", "acct-grand", 3)
g = repo.get_stream_state("acct-grand")
check("fork of fork", ff.balance == 90 and g.next_sequence == 3
      and g.pending_sequences == [], str((ff, g)))

# 11. concurrency: 20 threads, same fork_id/params -> one fork, all replayed
results, errors = [], []
latch = threading.Barrier(20)


def same_fork():
    try:
        latch.wait()
        results.append(forks.create_fork("acct", "f-conc", "acct-conc", 2))
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=same_fork) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent same fork no errors", not errors, str(errors[:1]))
check("concurrent same fork one winner, rest replay",
      len(results) == 20 and sum(1 for r in results if not r.replayed) == 1
      and sum(1 for r in results if r.replayed) == 19
      and len({r.target_stream_id for r in results}) == 1,
      f"replayed={sum(r.replayed for r in results)}")
cc = repo.get_stream_state("acct-conc")
check("concurrent fork state", cc.balance == 70 and cc.next_sequence == 2, str(cc))

# 12. concurrency: distinct fork_ids racing for the same target -> exactly one
# fork exists, no half-built stream/events left behind by losers.
winner = {"n": 0}
lock = threading.Lock()
race_errors = []
latch2 = threading.Barrier(20)


def same_target(i):
    try:
        latch2.wait()
        r = forks.create_fork("acct", f"f-race-{i}", "acct-race", 1)
        with lock:
            winner["n"] += 1
    except forks.ForkError as e:
        with lock:
            race_errors.append(e.code)
    except Exception:
        with lock:
            race_errors.append(traceback.format_exc())


threads = [threading.Thread(target=same_target, args=(i,)) for i in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("race one winner", winner["n"] == 1, str(winner))
check("race losers all target_stream_exists",
      len(race_errors) == 19 and set(race_errors) == {"target_stream_exists"},
      str(set(race_errors)))
rc = repo.get_stream_state("acct-race")
check("race target intact", rc.balance == 100 and rc.next_sequence == 1, str(rc))
with db.read_connection() as conn:
    n_forks = conn.execute(
        "SELECT COUNT(*) FROM forks WHERE target_stream_id = 'acct-race'"
    ).fetchone()[0]
    n_events = conn.execute(
        "SELECT COUNT(*) FROM events WHERE stream_id = 'acct-race'"
    ).fetchone()[0]
check("race no half-written rows", n_forks == 1 and n_events == 1,
      f"forks={n_forks} events={n_events}")

# 13. source stream is untouched by forking (still 105/5)
check("source unchanged", repo.get_stream_state("acct").balance == 105
      and repo.get_stream_state("acct").next_sequence == 5)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL FORK CHECKS PASSED")
