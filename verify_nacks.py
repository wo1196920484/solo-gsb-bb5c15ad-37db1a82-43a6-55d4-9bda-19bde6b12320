"""Stdlib-only smoke test for consumer nack / dead-lettering (no FastAPI needed)."""

import os
import tempfile
import threading
import traceback

tmp = tempfile.mkdtemp()
os.environ["DATABASE_PATH"] = os.path.join(tmp, "nacks.db")

from app import config, consumers, db, repo  # noqa: E402

db.initialise()

failures = []


def check(name, cond, extra=""):
    print(f"{'PASS' if cond else 'FAIL'}: {name} {extra}")
    if not cond:
        failures.append(name)


def expect_lease_error(name, fn, code):
    try:
        fn()
        check(name, False, "no error raised")
    except consumers.LeaseError as e:
        check(name, e.code == code, e.code)


def seed(stream_id, count):
    for seq in range(count):
        repo.submit_event(stream_id, f"{stream_id}-e{seq}", seq, (seq + 1) * 10)


def claim_nack(stream, consumer, reason, max_events=10):
    batch = consumers.claim(stream, consumer, max_events)
    result = consumers.nack(stream, consumer, batch.lease_token, reason)
    return batch, result


# 1. nack below the threshold: cursor stays, head re-claimed; default is 3.
check("default max attempts", config.consumer_max_attempts() == 3)
seed("s1", 3)
batch = consumers.claim("s1", "w", 2)
check("first batch head", [e.sequence for e in batch.events] == [0, 1])
r = consumers.nack("s1", "w", batch.lease_token, "boom")
check(
    "nack 1/3 keeps cursor",
    (r.attempts, r.max_attempts, r.dead_lettered, r.cursor, r.replayed) == (1, 3, False, 0, False),
    str(r),
)
# 2. same token retry replays the original result, never counts twice. This is
#    a plain HTTP retry: no newer claim has happened yet.
replay = consumers.nack("s1", "w", batch.lease_token, "different reason body")
check(
    "same token replay",
    replay.replayed and replay.attempts == 1 and replay.reason == "boom"
    and replay.dead_lettered is False and replay.cursor == 0
    and replay.lease_token == batch.lease_token,
    str(replay),
)
# Once a newer lease is claimed, the settled token is stale and rejected (409).
again = consumers.claim("s1", "w", 2)
check("re-claim returns same head", [e.sequence for e in again.events] == [0, 1])
check("re-claim issues new token", again.lease_token != batch.lease_token)
expect_lease_error(
    "settled token after newer claim",
    lambda: consumers.nack("s1", "w", batch.lease_token, "late"),
    "lease_token_mismatch",
)
check("no dead letters yet", consumers.list_dead_letters("s1", "w") == [])

# 3. attempts 2 and 3: the third nack dead-letters the head and skips one seq.
_, r2 = claim_nack("s1", "w", "boom-2")
check("nack 2/3", (r2.attempts, r2.dead_lettered, r2.cursor) == (2, False, 0))
b3, r3 = claim_nack("s1", "w", "boom-3")
check(
    "nack 3/3 dead-letters and skips exactly one sequence",
    (r3.attempts, r3.dead_lettered, r3.cursor) == (3, True, 1),
    str(r3),
)
# retrying the threshold token replays without creating a second DL row
r3retry = consumers.nack("s1", "w", b3.lease_token, "ignored")
check("threshold token replay", r3retry.replayed and r3retry.dead_lettered)

nxt = consumers.claim("s1", "w", 10)
check("claim continues after skipped event", [e.sequence for e in nxt.events] == [1, 2])
cursor = consumers.ack("s1", "w", nxt.lease_token)
check("ack rest", cursor == 3)

dls = consumers.list_dead_letters("s1", "w")
check(
    "one dead letter recorded",
    len(dls) == 1
    and (
        dls[0].sequence,
        dls[0].event_id,
        dls[0].delta,
        dls[0].attempts,
        dls[0].last_reason,
    )
    == (0, "s1-e0", 10, 3, "boom-3"),
    str([d.as_dict() for d in dls]),
)
check("dead letter isolation time set", dls[0].dead_lettered_at > 0)
check(
    "dead letters sorted by sequence ascending",
    [d.sequence for d in consumers.list_dead_letters("s1", "w")] == [0],
)

# 4. multiple poisoned heads accumulate in sequence order.
seed("s2", 3)
for head in range(3):
    for _ in range(3):
        batch = consumers.claim("s2", "w", 1)
        check(
            f"s2 head {head} re-offered",
            [e.sequence for e in batch.events] == [head],
        )
        consumers.nack("s2", "w", batch.lease_token, f"head-{head}")
done = consumers.claim("s2", "w", 10)
check("all three heads skipped", done.events == [])
dls2 = consumers.list_dead_letters("s2", "w")
check(
    "three dead letters in order",
    [(d.sequence, d.attempts, d.last_reason) for d in dls2]
    == [(0, 3, "head-0"), (1, 3, "head-1"), (2, 3, "head-2")],
    str([(d.sequence, d.attempts) for d in dls2]),
)

# 5. forged token with a live lease -> mismatch, lease survives.
seed("s3", 2)
live = consumers.claim("s3", "w", 2)
expect_lease_error(
    "forged token", lambda: consumers.nack("s3", "w", "bogus", "r"), "lease_token_mismatch"
)
check(
    "genuine lease still ackable",
    consumers.ack("s3", "w", live.lease_token) == 2,
)

# 6. never claimed -> lease_not_active.
expect_lease_error(
    "no lease", lambda: consumers.nack("s3", "fresh", "t", "r"), "lease_not_active"
)

# 7. already-nacked token after a newer claim -> stale token (409).
seed("s4", 2)
b_old = consumers.claim("s4", "w", 2)
consumers.nack("s4", "w", b_old.lease_token, "first")
consumers.claim("s4", "w", 2)
expect_lease_error(
    "old settled token after reclaim",
    lambda: consumers.nack("s4", "w", b_old.lease_token, "again"),
    "lease_token_mismatch",
)

# 8. expired lease token -> lease_expired, no failure counted.
seed("s5", 2)
b_exp = consumers.claim("s5", "w", 2)
real_now = consumers._now_ms
consumers._now_ms = lambda: b_exp.expires_at + 1
expect_lease_error(
    "expired nack", lambda: consumers.nack("s5", "w", b_exp.lease_token, "late"), "lease_expired"
)
expect_lease_error(
    "expired ack", lambda: consumers.ack("s5", "w", b_exp.lease_token), "lease_expired"
)
consumers._now_ms = real_now
b_new = consumers.claim("s5", "w", 2)
r = consumers.nack("s5", "w", b_new.lease_token, "fresh")
check("expired nack did not count", r.attempts == 1 and r.dead_lettered is False, str(r))

# 9. empty batch cannot be nacked; lease survives and is still ackable.
repo.submit_event("s6", "s6-e1", 1, 5)  # buffered behind gap at 0
empty = consumers.claim("s6", "w", 5)
check("empty batch", empty.events == [])
expect_lease_error(
    "empty batch nack",
    lambda: consumers.nack("s6", "w", empty.lease_token, "nothing"),
    "empty_batch_not_nackable",
)
same = consumers.claim("s6", "w", 5)
check("empty-batch lease retained", same.lease_token == empty.lease_token)
check("empty-batch lease ackable", consumers.ack("s6", "w", empty.lease_token) == 0)

# 10. ack resets the consecutive failure streak.
seed("s7", 2)
b = consumers.claim("s7", "w", 2)
consumers.nack("s7", "w", b.lease_token, "once")
b = consumers.claim("s7", "w", 2)
check("ack after one nack", consumers.ack("s7", "w", b.lease_token) == 2)
repo.submit_event("s7", "s7-e2", 2, 30)
b = consumers.claim("s7", "w", 2)
r = consumers.nack("s7", "w", b.lease_token, "later")
check("streak reset by ack", r.attempts == 1, str(r))

# 11. dead letters are isolated per consumer.
seed("s8", 1)
for _ in range(3):
    batch = consumers.claim("s8", "worker-a", 5)
    consumers.nack("s8", "worker-a", batch.lease_token, "a-no")
check(
    "worker-a dead-lettered",
    [d.sequence for d in consumers.list_dead_letters("s8", "worker-a")] == [0],
)
bb = consumers.claim("s8", "worker-b", 5)
check("worker-b still gets event", [e.sequence for e in bb.events] == [0])
check("worker-b acks fine", consumers.ack("s8", "worker-b", bb.lease_token) == 1)
check("worker-b has no dead letters", consumers.list_dead_letters("s8", "worker-b") == [])

# 11b. a settled token cannot be replayed through a different consumer path.
seed("s8b", 1)
bb = consumers.claim("s8b", "c-a", 5)
consumers.nack("s8b", "c-a", bb.lease_token, "from-a")
expect_lease_error(
    "settled token replayed via another consumer",
    lambda: consumers.nack("s8b", "c-b", bb.lease_token, "from-b-pretending"),
    "lease_not_active",
)

# 12. unknown stream / never-seen consumer handling.
check("nack unknown stream -> None", consumers.nack("ghost", "w", "t", "r") is None)
check(
    "dead letters unknown stream -> None",
    consumers.list_dead_letters("ghost", "w") is None,
)
check(
    "unknown consumer -> empty list",
    consumers.list_dead_letters("s1", "never") == [],
)

# 13. configurable threshold via CONSUMER_MAX_ATTEMPTS.
os.environ["CONSUMER_MAX_ATTEMPTS"] = "1"
check("threshold env override", config.consumer_max_attempts() == 1)
seed("s9", 2)
b = consumers.claim("s9", "w", 5)
r = consumers.nack("s9", "w", b.lease_token, "once-is-enough")
check(
    "first nack with threshold 1 dead-letters",
    r.attempts == 1 and r.dead_lettered is True and r.cursor == 1,
    str(r),
)
nxt = consumers.claim("s9", "w", 5)
check("next event served", [e.sequence for e in nxt.events] == [1])
del os.environ["CONSUMER_MAX_ATTEMPTS"]
check("threshold restored", config.consumer_max_attempts() == 3)
# invalid env values fall back to the default of 3
os.environ["CONSUMER_MAX_ATTEMPTS"] = "0"
check("threshold 0 -> default 3", config.consumer_max_attempts() == 3)
os.environ["CONSUMER_MAX_ATTEMPTS"] = "nope"
check("threshold junk -> default 3", config.consumer_max_attempts() == 3)
del os.environ["CONSUMER_MAX_ATTEMPTS"]

# 14. concurrent nacks with the same live token settle exactly once.
seed("s10", 2)
batch = consumers.claim("s10", "w", 2)
errors = []


def do_nack():
    try:
        consumers.nack("s10", "w", batch.lease_token, "concurrent")
    except Exception:
        errors.append(traceback.format_exc())


threads = [threading.Thread(target=do_nack) for _ in range(20)]
for th in threads:
    th.start()
for th in threads:
    th.join()
check("concurrent nacks no errors", not errors, str(errors[:1]))
b = consumers.claim("s10", "w", 2)
r = consumers.nack("s10", "w", b.lease_token, "second")
check("concurrent nacks counted once", r.attempts == 2 and r.dead_lettered is False, str(r))
check(
    "no dead letters under threshold",
    consumers.list_dead_letters("s10", "w") == [],
)

print()
if failures:
    print(f"{len(failures)} FAILURES: {failures}")
    raise SystemExit(1)
print("ALL NACK/DEAD-LETTER CHECKS PASSED")
