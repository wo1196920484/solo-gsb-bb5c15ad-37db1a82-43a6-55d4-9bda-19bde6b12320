"""Stdlib-only smoke test for consumer nack retries and dead letters.

Runs without FastAPI installed, exercising app.consumers directly:
    python3 verify_nacks.py
"""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "nacks.db")
os.environ["CONSUMER_MAX_ATTEMPTS"] = "3"

from app import consumers, db, repo  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def seed(stream_id, count):
    for seq in range(count):
        repo.submit_event(stream_id, f"{stream_id}-e{seq}", seq, seq + 10)


def claim_nack(stream, consumer, reason, max_events=10):
    batch = consumers.claim(stream, consumer, max_events)
    return batch, consumers.nack(stream, consumer, batch.lease_token, reason)


# Seed two streams.
seed("acct", 4)      # sequences 0..3, deltas 10..13
seed("other", 2)

# 1. first nack: cursor unchanged, attempt 1, lease released and redeliverable
batch = consumers.claim("acct", "worker", 2)
check("first batch", [e.sequence for e in batch.events] == [0, 1])
r = consumers.nack("acct", "worker", batch.lease_token, "boom-1")
check(
    "first nack does not advance",
    (r.attempt, r.dead_lettered, r.cursor, r.max_attempts, r.replayed)
    == (1, False, 0, 3, False),
    str(r),
)
redelivered = consumers.claim("acct", "worker", 2)
check("fresh token after release", redelivered.lease_token != batch.lease_token)
check("same head redelivered", [e.sequence for e in redelivered.events] == [0, 1])

# 2. the already-nacked token now replays (never a recount); ack cannot reuse it
rep = consumers.nack("acct", "worker", batch.lease_token, "again")
check("old nacked token replays", rep.replayed and rep.attempt == 1, str(rep))
try:
    consumers.ack("acct", "worker", batch.lease_token)
    check("ack after nack rejected", False)
except consumers.LeaseError as exc:
    check("ack after nack rejected", exc.code in ("lease_not_active", "lease_token_mismatch"), exc.code)

# 3. same-token retry returns the original result and never counts twice
replay = consumers.nack("acct", "worker", redelivered.lease_token, "boom-2")
check("second nack attempt", replay.attempt == 2 and not replay.dead_lettered, str(replay))
again = consumers.nack("acct", "worker", redelivered.lease_token, "different reason")
check(
    "same token replay",
    again.replayed and again.attempt == 2 and not again.dead_lettered and again.cursor == 0,
    str(again),
)
check("no dead letter yet", consumers.list_dead_letters("acct", "worker") == [])

# 4. forged token while a lease is live -> 409 mismatch, the live lease survives
live = consumers.claim("acct", "worker", 2)
try:
    consumers.nack("acct", "worker", "forged-token-value", "x")
    check("forged token rejected", False)
except consumers.LeaseError as exc:
    check("forged token rejected", exc.code == "lease_token_mismatch", exc.code)
# The real lease is still valid: acknowledge it, advancing past events 0 and 1.
check("lease survives forged nack", consumers.ack("acct", "worker", live.lease_token) == 2)

# 5. successful ack reset the consecutive-failure counter for the new head
batch = consumers.claim("acct", "worker", 10)
check("batch after ack", [e.sequence for e in batch.events] == [2, 3])
r = consumers.nack("acct", "worker", batch.lease_token, "post-ack fail")
check("failure after ack restarts at 1", r.attempt == 1 and r.head_sequence == 2, str(r))

# 6. third consecutive nack dead-letters the head event and skips it atomically
b2, r2 = claim_nack("acct", "worker", "r2")
check("attempt 2", r2.attempt == 2 and not r2.dead_lettered)
b3, r3 = claim_nack("acct", "worker", "r3-final")
check(
    "attempt 3 dead-letters and skips",
    r3.attempt == 3 and r3.dead_lettered and r3.cursor == 3 and not r3.replayed,
    str(r3),
)
after = consumers.claim("acct", "worker", 10)
check("next claim starts at 3", [e.sequence for e in after.events] == [3])

# 7. dead-letter log: event, attempts, last reason and quarantine timestamp
records = consumers.list_dead_letters("acct", "worker")
check("one dead letter", len(records) == 1)
dl = records[0]
check(
    "dead letter content",
    (dl.sequence, dl.event_id, dl.delta, dl.attempts, dl.last_reason)
    == (2, "acct-e2", 12, 3, "r3-final"),
    str(dl),
)
check("quarantine time set", dl.quarantined_at > 0)

# 8. final nack token replays as a dead-lettering outcome
rep = consumers.nack("acct", "worker", b3.lease_token, "retry")
check(
    "dead-lettering token replays",
    rep.replayed and rep.dead_lettered and rep.attempt == 3 and rep.cursor == 3,
    str(rep),
)
# replay must not create a second dead letter
check("replay does not duplicate", len(consumers.list_dead_letters("acct", "worker")) == 1)

# 9. dead letters accumulate per consumer in ascending sequence order
b, rr = claim_nack("acct", "worker", "kill-3")
for _ in range(2):
    b, rr = claim_nack("acct", "worker", "kill-3")
check("sequence 3 dead-lettered", rr.dead_lettered and rr.cursor == 4, str(rr))
records = consumers.list_dead_letters("acct", "worker")
check(
    "dead letters ascending",
    [d.sequence for d in records] == [2, 3]
    and [d.attempts for d in records] == [3, 3]
    and records[-1].last_reason == "kill-3",
    str([(d.sequence, d.last_reason) for d in records]),
)
# nothing left to consume
last = consumers.claim("acct", "worker", 10)
check("stream drained", last.events == [])

# 10. empty batch cannot be nacked (stream exists with only a buffered gap event)
repo.submit_event("gap-stream", "g1", 1, 1)  # sequence 0 missing -> nothing applied
empty = consumers.claim("gap-stream", "lonely", 10)
check("empty batch", empty.events == [])
try:
    consumers.nack("gap-stream", "lonely", empty.lease_token, "nothing here")
    check("empty batch nack rejected", False)
except consumers.LeaseError as exc:
    check("empty batch nack rejected", exc.code == "empty_batch", exc.code)
# empty batch can still be acked
check("empty batch ack ok", consumers.ack("gap-stream", "lonely", empty.lease_token) == 0)

# 11. never-claimed consumer -> lease_not_active
try:
    consumers.nack("gap-stream", "fresh", "whatever", "x")
    check("no lease rejected", False)
except consumers.LeaseError as exc:
    check("no lease rejected", exc.code == "lease_not_active", exc.code)

# 12. expired token -> lease_expired; reclaimable with a new token
import time as _time

exp = consumers.claim("other", "worker", 10)
_future = exp.expires_at + 1
consumers._now_ms = lambda: _future
try:
    consumers.nack("other", "worker", exp.lease_token, "late")
    check("expired rejected", False)
except consumers.LeaseError as exc:
    check("expired rejected", exc.code == "lease_expired", exc.code)
# Reclaim while the old lease is still past expiry -> new token, same batch.
reclaimed = consumers.claim("other", "worker", 10)
check("reclaim after expiry", reclaimed.lease_token != exp.lease_token and len(reclaimed.events) == 2)
consumers._now_ms = lambda: _time.time_ns() // 1_000_000

# 13. consumers are isolated: another consumer sees neither failures nor dead letters
check("isolated dead letters", consumers.list_dead_letters("acct", "someone-else") == [])
other_worker = consumers.claim("acct", "someone-else", 10)
check("isolated cursor", [e.sequence for e in other_worker.events] == [0, 1, 2, 3])

# 14. unknown stream -> None (HTTP 404 at the API layer); unknown consumer -> []
check("nack unknown stream", consumers.nack("ghost", "w", "t", "r") is None)
check("dead letters unknown stream", consumers.list_dead_letters("ghost", "w") is None)
check("dead letters unknown consumer", consumers.list_dead_letters("acct", "ghost") == [])

# 15. concurrency: 20 parallel nacks with the same token -> exactly one counts
cbatch = consumers.claim("acct", "race", 10)
outcomes: list[str] = []
lock = threading.Lock()


def race():
    try:
        result = consumers.nack("acct", "race", cbatch.lease_token, "race")
        with lock:
            outcomes.append("replay" if result.replayed else "first")
    except Exception:  # noqa: BLE001
        with lock:
            outcomes.append("error:" + traceback.format_exc())


threads = [threading.Thread(target=race) for _ in range(20)]
for t in threads:
    t.start()
for t in threads:
    t.join()
check("exactly one first nack", outcomes.count("first") == 1, str(outcomes))
check("nineteen replays", outcomes.count("replay") == 19)
check("no errors", not any(o.startswith("error") for o in outcomes))
check("no dead letter from the race", consumers.list_dead_letters("acct", "race") == [])
followup = claim_nack("acct", "race", "second")[1]
check("race counted once", followup.attempt == 2, str(followup))

# 16. CONSUMER_MAX_ATTEMPTS override (invalid values fall back to 3)
os.environ["CONSUMER_MAX_ATTEMPTS"] = "1"
b1 = consumers.claim("other", "env", 10)
r = consumers.nack("other", "env", b1.lease_token, "only chance")
check("threshold override 1", r.max_attempts == 1 and r.attempt == 1 and r.dead_lettered, str(r))
check(
    "override dead letter stored",
    [d.sequence for d in consumers.list_dead_letters("other", "env")] == [0],
)
os.environ["CONSUMER_MAX_ATTEMPTS"] = "not-a-number"
b2 = consumers.claim("other", "env", 10)
r = consumers.nack("other", "env", b2.lease_token, "x")
check("invalid env falls back to 3", r.max_attempts == 3, str(r))

print()
if failures:
    print(f"{len(failures)} FAILURE(S): {failures}")
    raise SystemExit(1)
print("all nack/dead-letter checks passed")
