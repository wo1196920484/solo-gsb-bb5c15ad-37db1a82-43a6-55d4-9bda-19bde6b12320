"""Stdlib-only smoke test for the merge core (run without FastAPI installed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "verify.db")

from app import db, repo  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


# 1. ordered accumulation
s, rep = repo.submit_event("s1", "e0", 0, 100)
check("first insert 201-ish replayed=False", not rep)
check("ordered balance 100", s.balance == 100 and s.next_sequence == 1 and s.pending_sequences == [], str(s))
s, rep = repo.submit_event("s1", "e1", 1, -30)
check("ordered balance 70", s.balance == 70 and s.next_sequence == 1 + 1)

# 2. gap buffered
s, _ = repo.submit_event("s2", "e1", 1, 200)
check("gap buffered", s.balance == 0 and s.next_sequence == 0 and s.pending_sequences == [1], str(s))
s, _ = repo.submit_event("s2", "e0", 0, 50)
check("catch-up applied both", s.balance == 250 and s.next_sequence == 2 and s.pending_sequences == [], str(s))

# 3. idempotent replay
s, rep = repo.submit_event("s1", "e0", 0, 100)
check("replay idempotent", rep and s.balance == 70 and s.next_sequence == 2, str(s))

# 4. replay conflict
try:
    repo.submit_event("s1", "e0", 1, 100)
    check("replay diff seq -> 409", False)
except repo.ConflictError as e:
    check("replay diff seq -> 409", e.code == "replay_conflict")
try:
    repo.submit_event("s1", "e0", 0, 999)
    check("replay diff delta -> 409", False)
except repo.ConflictError:
    check("replay diff delta -> 409", True)

# 5. duplicate sequence different event_id
try:
    repo.submit_event("s1", "other", 0, 100)
    check("dup sequence -> 409", False)
except repo.ConflictError as e:
    check("dup sequence -> 409", e.code == "sequence_conflict")

# 6. unknown stream
check("unknown stream None", repo.get_stream_state("nope") is None)

# 7. negative sequence rejected by DB CHECK
import sqlite3
try:
    repo.submit_event("s3", "ex", -1, 1)
    check("negative sequence rejected", False)
except sqlite3.IntegrityError:
    check("negative sequence rejected", True)

# 8. large out-of-order batch
for seq in range(1, 10):
    s, _ = repo.submit_event("s4", f"e{seq}", seq, seq)
check("batch pending", s.pending_sequences == list(range(1, 10)), str(s))
s, _ = repo.submit_event("s4", "e0", 0, 0)
check("batch catch-up", s.next_sequence == 10 and s.balance == sum(range(10)) and s.pending_sequences == [], str(s))

# 9. concurrency: 20 threads, scrambled order
errors = []
def post(seq):
    try:
        repo.submit_event("s5", f"e{seq}", seq, 1)
    except Exception:
        errors.append(traceback.format_exc())

threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent no errors", not errors, str(errors[:1]))
s = repo.get_stream_state("s5")
check("concurrent result", s.balance == 20 and s.next_sequence == 20 and s.pending_sequences == [], str(s))

# 10. persistence: reopen connection and re-read
s = repo.get_stream_state("s2")
check("persisted read", s.balance == 250 and s.next_sequence == 2)

# 11. consumer cursor: claim / lease / ack
from app import consumers  # noqa: E402

# applied events on s5: sequences 0..19
c = consumers.claim("s5", "w1", 5)
check("claim first 5", [e.sequence for e in c.events] == [0, 1, 2, 3, 4]
      and c.from_sequence == 0 and c.to_sequence == 5, str(c))
token1 = c.lease_token
expires1 = c.expires_at

# re-claim within the lease -> identical batch and token, regardless of max_events
c2 = consumers.claim("s5", "w1", 500)
check("re-claim same batch/token", c2.lease_token == token1 and c2.expires_at == expires1
      and [e.sequence for e in c2.events] == [0, 1, 2, 3, 4], str(c2))

# another consumer has its own independent cursor
c_other = consumers.claim("s5", "w2", 2)
check("independent consumer cursor", [e.sequence for e in c_other.events] == [0, 1]
      and c_other.lease_token != token1, str(c_other))

# wrong token -> 409
try:
    consumers.ack("s5", "w1", "bogus-token")
    check("ack wrong token -> 409", False)
except consumers.LeaseError as e:
    check("ack wrong token -> 409", e.code == "lease_token_mismatch")

# ack without claim -> 409
try:
    consumers.ack("s5", "w-never", "x")
    check("ack without lease -> 409", False)
except consumers.LeaseError as e:
    check("ack without lease -> 409", e.code == "lease_not_active")

# valid ack advances cursor
new_cursor = consumers.ack("s5", "w1", token1)
check("ack advances cursor", new_cursor == 5, str(new_cursor))
c3 = consumers.claim("s5", "w1", 5)
check("next claim continues at cursor", [e.sequence for e in c3.events] == [5, 6, 7, 8, 9], str(c3))

# unknown stream -> None (HTTP 404 at the API layer)
check("claim unknown stream None", consumers.claim("nope", "w", 1) is None)
check("ack unknown stream None", consumers.ack("nope", "w", "x") is None)

# 12. expired lease: ack rejected, batch re-claimable with a new token
ce = consumers.claim("s5", "w3", 3)
real_now = consumers._now_ms
consumers._now_ms = lambda: ce.expires_at + 1
try:
    try:
        consumers.ack("s5", "w3", ce.lease_token)
        check("expired token -> 409", False)
    except consumers.LeaseError as e:
        check("expired token -> 409", e.code == "lease_expired")
    ce2 = consumers.claim("s5", "w3", 3)
    check("expired batch re-claimed", ce2.lease_token != ce.lease_token
          and [e.sequence for e in ce2.events] == [0, 1, 2], str(ce2))
    check("new lease acks fine", consumers.ack("s5", "w3", ce2.lease_token) == 3)
finally:
    consumers._now_ms = real_now

# 13. concurrency: 20 simultaneous claims yield exactly one valid lease
tokens = []
lock_errors = []
def do_claim():
    try:
        tokens.append(consumers.claim("s5", "w4", 5).lease_token)
    except Exception:
        lock_errors.append(traceback.format_exc())

threads = [threading.Thread(target=do_claim) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("concurrent claims no errors", not lock_errors, str(lock_errors[:1]))
check("concurrent claims one lease", len(tokens) == 20 and len(set(tokens)) == 1, str(set(tokens)))

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL CORE CHECKS PASSED")
