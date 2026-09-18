"""End-to-end API tests (requires fastapi + httpx, installed via requirements-dev.txt)."""

from __future__ import annotations

import os
import tempfile

import pytest


@pytest.fixture(scope="session")
def client(tmp_path_factory):
    db_dir = tmp_path_factory.mktemp("db")
    os.environ["DATABASE_PATH"] = str(db_dir / "test_events.db")

    from fastapi.testclient import TestClient  # noqa: E402

    from app.main import app  # noqa: E402

    with TestClient(app) as c:
        yield c


def _post(client, stream_id, event_id, sequence, delta):
    return client.post(
        f"/streams/{stream_id}/events",
        json={"event_id": event_id, "sequence": sequence, "delta": delta},
    )


def test_in_order_events_accumulate(client):
    r = _post(client, "s-ordered", "e0", 0, 100)
    assert r.status_code == 201
    assert r.json()["stream"] == {
        "stream_id": "s-ordered",
        "balance": 100,
        "next_sequence": 1,
        "pending_sequences": [],
    }

    r = _post(client, "s-ordered", "e1", 1, -30)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 70
    assert r.json()["stream"]["next_sequence"] == 2


def test_gap_is_buffered_then_caught_up_atomically(client):
    # Sequence 1 arrives before sequence 0: nothing is applied yet.
    r = _post(client, "s-gap", "e1", 1, 200)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 0
    assert body["stream"]["next_sequence"] == 0
    assert body["stream"]["pending_sequences"] == [1]

    r = client.get("/streams/s-gap")
    assert r.json()["pending_sequences"] == [1]

    # Missing event arrives: both events are applied in the same transaction.
    r = _post(client, "s-gap", "e0", 0, 50)
    assert r.status_code == 201
    body = r.json()
    assert body["stream"]["balance"] == 250
    assert body["stream"]["next_sequence"] == 2
    assert body["stream"]["pending_sequences"] == []


def test_replay_is_idempotent(client):
    _post(client, "s-rep", "e0", 0, 10)

    r = _post(client, "s-rep", "e0", 0, 10)
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert r.json()["stream"]["balance"] == 10
    assert r.json()["stream"]["next_sequence"] == 1


def test_replay_with_different_content_returns_409(client):
    _post(client, "s-conf", "e0", 0, 10)

    r = _post(client, "s-conf", "e0", 1, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "replay_conflict"

    r = _post(client, "s-conf", "e0", 0, 999)
    assert r.status_code == 409


def test_duplicate_sequence_different_event_id_returns_409(client):
    _post(client, "s-dup", "e0", 0, 10)

    r = _post(client, "s-dup", "other-event", 0, 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "sequence_conflict"
    # The conflicting event was never stored.
    assert client.get("/streams/s-dup").json()["next_sequence"] == 1


def test_get_unknown_stream_returns_404(client):
    r = client.get("/streams/does-not-exist")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_validation_rejects_bad_payloads(client):
    assert _post(client, "s-val", "", 0, 1).status_code == 422
    assert _post(client, "s-val", "e", -1, 1).status_code == 422
    assert _post(client, "s-val", "e", 0, "not-an-int").status_code == 422
    assert _post(client, "s-val", "e", True, 1).status_code == 422


def test_large_out_of_order_batch_catches_up(client):
    # Submit 1..9 (all buffered because 0 is missing), then 0.
    for seq in range(1, 10):
        r = _post(client, "s-batch", f"e{seq}", seq, seq)
        assert r.json()["stream"]["next_sequence"] == 0
    assert client.get("/streams/s-batch").json()["pending_sequences"] == list(range(1, 10))

    r = _post(client, "s-batch", "e0", 0, 0)
    body = r.json()
    assert body["stream"]["next_sequence"] == 10
    assert body["stream"]["balance"] == sum(range(10))
    assert body["stream"]["pending_sequences"] == []


def test_concurrent_submissions_apply_each_sequence_once(client):
    import threading

    def post(seq):
        _post(client, "s-conc", f"e{seq}", seq, 1)

    threads = [threading.Thread(target=post, args=(seq,)) for seq in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    state = client.get("/streams/s-conc").json()
    assert state["next_sequence"] == 20
    assert state["balance"] == 20
    assert state["pending_sequences"] == []


# --------------------------------------------------------------------------- #
# Consumer cursor: claim / ack leases
# --------------------------------------------------------------------------- #


def _seed_applied_events(client, stream_id, count, start=0):
    for seq in range(start, start + count):
        r = _post(client, stream_id, f"{stream_id}-e{seq}", seq, seq + 1)
        assert r.status_code in (200, 201)


def _claim(client, stream_id, consumer_id, max_events):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/claim",
        json={"max_events": max_events},
    )


def _ack(client, stream_id, consumer_id, token):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/ack",
        json={"lease_token": token},
    )


def test_claim_returns_contiguous_applied_batch(client):
    _seed_applied_events(client, "c-basic", 5)

    r = _claim(client, "c-basic", "worker-1", 3)
    assert r.status_code == 200
    body = r.json()
    assert body["from_sequence"] == 0
    assert body["to_sequence"] == 3
    assert [e["sequence"] for e in body["events"]] == [0, 1, 2]
    assert [e["delta"] for e in body["events"]] == [1, 2, 3]
    assert body["lease_token"]
    assert body["expires_at"] > 0


def test_claim_skips_events_not_yet_applied_due_to_gap(client):
    # Sequences 1 and 2 arrive first and stay buffered (0 is missing).
    _post(client, "c-gap", "c-gap-e1", 1, 10)
    _post(client, "c-gap", "c-gap-e2", 2, 20)

    r = _claim(client, "c-gap", "worker", 10)
    assert r.status_code == 200
    body = r.json()
    # Nothing is applied yet: empty batch, but a lease still exists.
    assert body["events"] == []
    assert body["from_sequence"] == body["to_sequence"] == 0

    # Sequence 0 arrives; only it is applied (1 and 2 still pending the old gap?
    # actually all three become contiguous -> applied 0,1,2).
    _post(client, "c-gap", "c-gap-e0", 0, 5)

    # Expire the empty-batch lease, then re-claim.
    from app import consumers

    consumers._now_ms = lambda: body["expires_at"] + 1
    r = _claim(client, "c-gap", "worker", 10)
    assert [e["sequence"] for e in r.json()["events"]] == [0, 1, 2]


def test_reclaim_within_lease_returns_same_batch_and_token(client):
    _seed_applied_events(client, "c-same", 4)

    first = _claim(client, "c-same", "worker", 2).json()
    second = _claim(client, "c-same", "worker", 999).json()
    # Same batch, same token, same expiry, regardless of the new max_events.
    assert second == first


def test_ack_advances_cursor_and_next_claim_continues(client):
    _seed_applied_events(client, "c-ack", 5)

    batch = _claim(client, "c-ack", "worker", 2).json()
    r = _ack(client, "c-ack", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 2

    next_batch = _claim(client, "c-ack", "worker", 10).json()
    assert next_batch["from_sequence"] == 2
    assert [e["sequence"] for e in next_batch["events"]] == [2, 3, 4]


def test_ack_with_wrong_token_returns_409_and_keeps_lease(client):
    _seed_applied_events(client, "c-wrong", 2)
    _claim(client, "c-wrong", "worker", 2)

    r = _ack(client, "c-wrong", "worker", "not-the-real-token")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    # The valid lease is still usable and still covers the same batch.
    batch = _claim(client, "c-wrong", "worker", 2).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]
    assert _ack(client, "c-wrong", "worker", batch["lease_token"]).status_code == 200


def test_ack_without_claim_returns_409(client):
    _seed_applied_events(client, "c-nolease", 1)
    r = _ack(client, "c-nolease", "fresh-worker", "anything")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_active"


def test_old_token_after_reclaim_is_rejected_with_409(client):
    _seed_applied_events(client, "c-old", 2)

    first = _claim(client, "c-old", "worker", 2).json()
    from app import consumers

    consumers._now_ms = lambda: first["expires_at"] + 1
    second = _claim(client, "c-old", "worker", 2).json()
    assert second["lease_token"] != first["lease_token"]

    # The old (expired) token must not advance the cursor.
    r = _ack(client, "c-old", "worker", first["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    assert _ack(client, "c-old", "worker", second["lease_token"]).status_code == 200


def test_expired_lease_token_is_rejected_but_batch_reclaimable(client):
    _seed_applied_events(client, "c-exp", 2)
    batch = _claim(client, "c-exp", "worker", 2).json()

    from app import consumers

    consumers._now_ms = lambda: batch["expires_at"] + 1

    r = _ack(client, "c-exp", "worker", batch["lease_token"])
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"

    # Lease expired: the same batch is handed out again with a new token.
    again = _claim(client, "c-exp", "worker", 2).json()
    assert [e["sequence"] for e in again["events"]] == [0, 1]
    assert again["lease_token"] != batch["lease_token"]
    assert _ack(client, "c-exp", "worker", again["lease_token"]).json()["cursor"] == 2


def test_two_consumers_have_independent_cursors(client):
    _seed_applied_events(client, "c-two", 3)

    b1 = _claim(client, "c-two", "worker-a", 1).json()
    b2 = _claim(client, "c-two", "worker-b", 1).json()
    assert b1["events"][0]["sequence"] == 0
    assert b2["events"][0]["sequence"] == 0
    assert b1["lease_token"] != b2["lease_token"]

    assert _ack(client, "c-two", "worker-a", b1["lease_token"]).json()["cursor"] == 1
    assert _ack(client, "c-two", "worker-b", b2["lease_token"]).json()["cursor"] == 1


def test_claim_on_unknown_stream_returns_404(client):
    r = _claim(client, "no-such-stream", "worker", 10)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_ack_on_unknown_stream_returns_404(client):
    r = _ack(client, "no-such-stream", "worker", "token")
    assert r.status_code == 404


def test_claim_validation_rejects_bad_max_events(client):
    assert _claim(client, "c-val", "worker", 0).status_code == 422
    assert _claim(client, "c-val", "worker", -1).status_code == 422
    assert _claim(client, "c-val", "worker", True).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/claim", json={}
    ).status_code == 422
    assert client.post(
        "/streams/c-val/consumers/worker/ack", json={}
    ).status_code == 422


def test_concurrent_claims_never_produce_two_valid_leases(client):
    import threading

    _seed_applied_events(client, "c-conc", 10)
    tokens: list[str] = []

    def do_claim():
        r = _claim(client, "c-conc", "worker", 5)
        tokens.append(r.json()["lease_token"])

    threads = [threading.Thread(target=do_claim) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every concurrent claimant observed the exact same valid lease.
    assert len(tokens) == 20
    assert len(set(tokens)) == 1

    batch = _claim(client, "c-conc", "worker", 5).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1, 2, 3, 4]
    assert _ack(client, "c-conc", "worker", tokens[0]).json()["cursor"] == 5


def test_events_arriving_after_claim_appear_after_ack(client):
    _seed_applied_events(client, "c-late", 2)
    batch = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1]

    # New events land while the batch is leased; they do not change the lease.
    _seed_applied_events(client, "c-late", 2, start=2)
    same = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in same["events"]] == [0, 1]

    _ack(client, "c-late", "worker", batch["lease_token"])
    after = _claim(client, "c-late", "worker", 10).json()
    assert [e["sequence"] for e in after["events"]] == [2, 3]


def test_ack_of_empty_batch_advances_nothing(client):
    # Stream exists but no applied events.
    _post(client, "c-empty", "c-empty-e1", 1, 1)  # buffered, gap at 0
    batch = _claim(client, "c-empty", "worker", 5).json()
    assert batch["events"] == []
    assert batch["from_sequence"] == batch["to_sequence"] == 0

    r = _ack(client, "c-empty", "worker", batch["lease_token"])
    assert r.status_code == 200
    assert r.json()["cursor"] == 0


# --------------------------------------------------------------------------- #
# Consumer nack: retries, dead-lettering and the dead-letter log
# --------------------------------------------------------------------------- #


def _nack(client, stream_id, consumer_id, token, reason="processing failed"):
    return client.post(
        f"/streams/{stream_id}/consumers/{consumer_id}/nack",
        json={"lease_token": token, "reason": reason},
    )


def _dead_letters(client, stream_id, consumer_id):
    return client.get(
        f"/streams/{stream_id}/consumers/{consumer_id}/dead-letters"
    )


def test_nack_below_threshold_keeps_cursor_and_reclaims_head(client):
    _seed_applied_events(client, "n-basic", 3)
    batch = _claim(client, "n-basic", "worker", 2).json()

    r = _nack(client, "n-basic", "worker", batch["lease_token"], "bad-luck")
    assert r.status_code == 200
    body = r.json()
    assert body["attempts"] == 1
    assert body["max_attempts"] == 3
    assert body["dead_lettered"] is False
    assert body["replayed"] is False
    assert body["cursor"] == 0
    assert body["reason"] == "bad-luck"

    # Lease was released immediately; the next claim starts at the same cursor.
    again = _claim(client, "n-basic", "worker", 2).json()
    assert again["lease_token"] != batch["lease_token"]
    assert [e["sequence"] for e in again["events"]] == [0, 1]

    # Nothing has been dead-lettered yet.
    d = _dead_letters(client, "n-basic", "worker")
    assert d.status_code == 200
    assert d.json()["dead_letters"] == []


def test_nack_threshold_dead_letters_head_and_advances_cursor_one_step(client):
    _seed_applied_events(client, "n-dlq", 4)
    token = None
    for attempt in range(1, 4):
        batch = _claim(client, "n-dlq", "worker", 2).json()
        assert [e["sequence"] for e in batch["events"]] == [0, 1]
        token = batch["lease_token"]
        r = _nack(client, "n-dlq", "worker", token, f"why-{attempt}")
        assert r.json()["attempts"] == attempt

    last = r.json()
    assert last["attempts"] == 3
    assert last["dead_lettered"] is True
    # Cursor moved one sequence past the dead head event only.
    assert last["cursor"] == 1

    # The next claim continues from sequence 1.
    nxt = _claim(client, "n-dlq", "worker", 10).json()
    assert [e["sequence"] for e in nxt["events"]] == [1, 2, 3]

    d = _dead_letters(client, "n-dlq", "worker").json()["dead_letters"]
    assert len(d) == 1
    row = d[0]
    assert row["sequence"] == 0
    assert row["event_id"] == "n-dlq-e0"
    assert row["delta"] == 1
    assert row["attempts"] == 3
    assert row["last_reason"] == "why-3"
    assert row["dead_lettered_at"] > 0


def test_nack_retry_with_same_token_replays_without_double_counting(client):
    _seed_applied_events(client, "n-rep", 2)
    batch = _claim(client, "n-rep", "worker", 2).json()

    first = _nack(client, "n-rep", "worker", batch["lease_token"], "r1")
    # Retry with the same token (and even a different reason body): the stored
    # original result is returned and the failure is not counted twice.
    retry = _nack(client, "n-rep", "worker", batch["lease_token"], "r2")
    assert retry.status_code == 200
    a, b = first.json(), retry.json()
    assert b["replayed"] is True
    assert b["attempts"] == 1
    assert b["reason"] == "r1"
    assert b["dead_lettered"] == a["dead_lettered"]
    assert b["cursor"] == a["cursor"]
    assert b["lease_token"] == a["lease_token"]

    # Replay also works at the threshold: exactly one dead-letter row is born.
    batch = _claim(client, "n-rep", "worker", 2).json()
    _nack(client, "n-rep", "worker", batch["lease_token"], "r2")
    batch = _claim(client, "n-rep", "worker", 2).json()
    third = _nack(client, "n-rep", "worker", batch["lease_token"], "r3")
    assert third.json()["dead_lettered"] is True
    retried = _nack(client, "n-rep", "worker", batch["lease_token"], "r4")
    assert retried.json()["replayed"] is True
    assert retried.json()["attempts"] == 3
    d = _dead_letters(client, "n-rep", "worker").json()["dead_letters"]
    assert [x["sequence"] for x in d] == [0]
    assert d[0]["attempts"] == 3
    assert d[0]["last_reason"] == "r3"


def test_nack_forged_token_returns_409_and_lease_stays_live(client):
    _seed_applied_events(client, "n-forge", 2)
    batch = _claim(client, "n-forge", "worker", 2).json()
    r = _nack(client, "n-forge", "worker", "totally-fake")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"

    # The genuine lease is untouched and still ackable.
    assert _ack(client, "n-forge", "worker", batch["lease_token"]).status_code == 200


def test_nack_without_claim_returns_409(client):
    _seed_applied_events(client, "n-nolease", 1)
    r = _nack(client, "n-nolease", "fresh-worker", "whatever")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_active"


def test_nack_old_settled_token_after_reclaim_returns_409(client):
    _seed_applied_events(client, "n-old", 3)
    first = _claim(client, "n-old", "worker", 2).json()
    _nack(client, "n-old", "worker", first["lease_token"], "first")

    second = _claim(client, "n-old", "worker", 2).json()
    assert second["lease_token"] != first["lease_token"]
    # The already-nacked token is stale relative to the current lease.
    r = _nack(client, "n-old", "worker", first["lease_token"], "again")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_token_mismatch"


def test_nack_expired_lease_returns_409_and_does_not_count(client):
    _seed_applied_events(client, "n-exp", 2)
    batch = _claim(client, "n-exp", "worker", 2).json()

    from app import consumers
    import time

    consumers._now_ms = lambda: batch["expires_at"] + 1
    r = _nack(client, "n-exp", "worker", batch["lease_token"], "late")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_expired"

    consumers._now_ms = lambda: int(time.time() * 1000)
    again = _claim(client, "n-exp", "worker", 2).json()
    r = _nack(client, "n-exp", "worker", again["lease_token"], "fresh")
    assert r.json()["attempts"] == 1
    assert _dead_letters(client, "n-exp", "worker").json()["dead_letters"] == []


def test_nack_empty_batch_returns_409_and_lease_survives(client):
    _post(client, "n-empty", "n-empty-e1", 1, 1)  # buffered behind gap at 0
    batch = _claim(client, "n-empty", "worker", 5).json()
    assert batch["events"] == []

    r = _nack(client, "n-empty", "worker", batch["lease_token"], "nothing here")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "empty_batch_not_nackable"

    # Lease is still live: re-claim returns the same token, and ack works.
    same = _claim(client, "n-empty", "worker", 5).json()
    assert same["lease_token"] == batch["lease_token"]
    assert _ack(client, "n-empty", "worker", batch["lease_token"]).status_code == 200


def test_nack_validation_rejects_blank_fields(client):
    _seed_applied_events(client, "n-val", 1)
    batch = _claim(client, "n-val", "worker", 2).json()
    assert client.post(
        f"/streams/n-val/consumers/worker/nack",
        json={"lease_token": batch["lease_token"], "reason": ""},
    ).status_code == 422
    assert client.post(
        f"/streams/n-val/consumers/worker/nack",
        json={"lease_token": batch["lease_token"], "reason": "   "},
    ).status_code == 422
    assert client.post(
        f"/streams/n-val/consumers/worker/nack",
        json={"lease_token": "  ", "reason": "x"},
    ).status_code == 422
    assert client.post(
        f"/streams/n-val/consumers/worker/nack",
        json={"lease_token": batch["lease_token"]},
    ).status_code == 422
    assert client.post(
        f"/streams/n-val/consumers/worker/nack",
        json={"reason": "x"},
    ).status_code == 422
    # The lease was never settled by the rejected requests.
    assert _ack(client, "n-val", "worker", batch["lease_token"]).status_code == 200


def test_nack_on_unknown_stream_returns_404(client):
    r = _nack(client, "no-such-stream", "worker", "token")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_dead_letters_unknown_stream_returns_404_unknown_consumer_empty(client):
    r = _dead_letters(client, "no-such-stream", "worker")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    _seed_applied_events(client, "n-dl-empty", 1)
    r = _dead_letters(client, "n-dl-empty", "never-claimed")
    assert r.status_code == 200
    assert r.json() == {
        "stream_id": "n-dl-empty",
        "consumer_id": "never-claimed",
        "dead_letters": [],
    }


def test_ack_resets_consecutive_nack_streak(client):
    _seed_applied_events(client, "n-reset", 2)
    batch = _claim(client, "n-reset", "worker", 2).json()
    assert _nack(client, "n-reset", "worker", batch["lease_token"]).json()["attempts"] == 1

    # Re-claim and acknowledge: the head event's failure streak is cleared.
    batch = _claim(client, "n-reset", "worker", 2).json()
    assert _ack(client, "n-reset", "worker", batch["lease_token"]).json()["cursor"] == 2

    # A later failure on the next batch starts counting from 1 again.
    _post(client, "n-reset", "n-reset-e2", 2, 3)
    batch = _claim(client, "n-reset", "worker", 2).json()
    assert [e["sequence"] for e in batch["events"]] == [2]
    assert _nack(client, "n-reset", "worker", batch["lease_token"]).json()["attempts"] == 1


def test_dead_lettering_one_head_does_not_block_later_events(client):
    _seed_applied_events(client, "n-move", 4)
    # Dead-letter sequence 0.
    for _ in range(3):
        batch = _claim(client, "n-move", "worker", 2).json()
        _nack(client, "n-move", "worker", batch["lease_token"], "poison")
    assert _dead_letters(client, "n-move", "worker").json()["dead_letters"][0]["sequence"] == 0

    # Remaining events flow through and ack cleanly.
    batch = _claim(client, "n-move", "worker", 10).json()
    assert [e["sequence"] for e in batch["events"]] == [1, 2, 3]
    assert _ack(client, "n-move", "worker", batch["lease_token"]).json()["cursor"] == 4

    assert [
        x["sequence"]
        for x in _dead_letters(client, "n-move", "worker").json()["dead_letters"]
    ] == [0]


def test_consecutive_nacks_dead_letter_multiple_heads_in_sequence_order(client):
    _seed_applied_events(client, "n-multi", 3)
    for head in range(3):
        for _ in range(3):
            batch = _claim(client, "n-multi", "worker", 1).json()
            assert [e["sequence"] for e in batch["events"]] == [head]
            _nack(client, "n-multi", "worker", batch["lease_token"], f"head-{head}")

    # Caught up by skipping all three; no more events to claim.
    done = _claim(client, "n-multi", "worker", 10).json()
    assert done["events"] == []
    d = _dead_letters(client, "n-multi", "worker").json()["dead_letters"]
    assert [x["sequence"] for x in d] == [0, 1, 2]
    assert all(x["attempts"] == 3 for x in d)


def test_dead_letters_are_isolated_per_consumer(client):
    _seed_applied_events(client, "n-iso", 1)
    # worker-a poisons the event; worker-b is unaffected and can ack it.
    for _ in range(3):
        batch = _claim(client, "n-iso", "worker-a", 5).json()
        _nack(client, "n-iso", "worker-a", batch["lease_token"], "a-says-no")
    batch = _claim(client, "n-iso", "worker-b", 5).json()
    assert [e["sequence"] for e in batch["events"]] == [0]
    assert _ack(client, "n-iso", "worker-b", batch["lease_token"]).status_code == 200

    da = _dead_letters(client, "n-iso", "worker-a").json()["dead_letters"]
    db_ = _dead_letters(client, "n-iso", "worker-b").json()["dead_letters"]
    assert [x["sequence"] for x in da] == [0]
    assert db_ == []


def test_settled_token_cannot_be_replayed_through_another_consumer(client):
    _seed_applied_events(client, "n-cross", 1)
    batch = _claim(client, "n-cross", "worker-a", 5).json()
    _nack(client, "n-cross", "worker-a", batch["lease_token"], "from-a")
    # The same token presented by a different (lease-less) consumer is not a replay.
    r = _nack(client, "n-cross", "worker-b", batch["lease_token"], "impostor")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "lease_not_active"
    assert _dead_letters(client, "n-cross", "worker-a").json()["dead_letters"] == []


def test_concurrent_nacks_of_same_token_settle_once(client):
    import threading

    _seed_applied_events(client, "n-conc", 2)
    batch = _claim(client, "n-conc", "worker", 2).json()
    results: list[int] = []

    def do_nack():
        r = _nack(client, "n-conc", "worker", batch["lease_token"], "concurrent")
        results.append(r.status_code)

    threads = [threading.Thread(target=do_nack) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results == [200] * 20
    # Exactly one failure counted; no dead letter and no duplicate outcome rows.
    again = _claim(client, "n-conc", "worker", 2).json()
    r = _nack(client, "n-conc", "worker", again["lease_token"], "second")
    assert r.json()["attempts"] == 2
    assert _dead_letters(client, "n-conc", "worker").json()["dead_letters"] == []


# --------------------------------------------------------------------------- #
# Stream forking
# --------------------------------------------------------------------------- #


def _fork(client, source_id, fork_id, target_id, through):
    return client.post(
        f"/streams/{source_id}/forks",
        json={
            "fork_id": fork_id,
            "target_stream_id": target_id,
            "through_sequence": through,
        },
    )


def _seed_for_fork(client, stream_id="f-src", deltas=(100, -30, 20, 10, 5)):
    for seq, delta in enumerate(deltas):
        assert _post(client, stream_id, f"{stream_id}-e{seq}", seq, delta).status_code in (
            200,
            201,
        )


def test_fork_copies_full_applied_prefix(client):
    _seed_for_fork(client, "f-src-full")
    r = _fork(client, "f-src-full", "fork-1", "f-dst", 5)
    assert r.status_code == 201
    assert r.json() == {
        "source_stream_id": "f-src-full",
        "target_stream_id": "f-dst",
        "through_sequence": 5,
        "balance": 105,
        "next_sequence": 5,
    }

    state = client.get("/streams/f-dst").json()
    assert state == {
        "stream_id": "f-dst",
        "balance": 105,
        "next_sequence": 5,
        "pending_sequences": [],
    }
    # The source is untouched.
    assert client.get("/streams/f-src-full").json()["balance"] == 105


def test_forked_stream_keeps_accepting_events(client):
    _seed_for_fork(client, "f-src-append")
    _fork(client, "f-src-append", "fork-1", "f-dst-append", 3)

    r = _post(client, "f-dst-append", "f-dst-append-e3", 3, 7)
    assert r.status_code == 201
    body = r.json()["stream"]
    assert body["next_sequence"] == 4
    assert body["balance"] == 97  # 100 - 30 + 20 + 7


def test_fork_partial_prefix(client):
    _seed_for_fork(client, "f-src-part")
    r = _fork(client, "f-src-part", "fork-part", "f-part", 2)
    assert r.status_code == 201
    body = r.json()
    assert body["through_sequence"] == 2
    assert body["balance"] == 70
    assert body["next_sequence"] == 2
    assert client.get("/streams/f-part").json()["pending_sequences"] == []


def test_fork_zero_through_sequence_copies_nothing(client):
    _seed_for_fork(client, "f-src-zero")
    r = _fork(client, "f-src-zero", "fork-zero", "f-zero", 0)
    assert r.status_code == 201
    assert r.json()["balance"] == 0
    assert r.json()["next_sequence"] == 0
    state = client.get("/streams/f-zero").json()
    assert state["balance"] == 0 and state["next_sequence"] == 0


def test_fork_skips_pending_unapplied_events(client):
    # Buffered pending events past the gap never enter the fork.
    _post(client, "f-gap-src", "e1", 1, 200)
    _post(client, "f-gap-src", "e2", 2, 50)
    _post(client, "f-gap-src", "e0", 0, 100)  # catch-up -> next_sequence=3
    _post(client, "f-gap-src", "e5", 5, 9)  # stays pending (gap at 3)

    r = _fork(client, "f-gap-src", "g", "f-gap-dst", 3)
    assert r.status_code == 201
    body = r.json()
    assert body["balance"] == 350 and body["next_sequence"] == 3
    assert client.get("/streams/f-gap-dst").json()["pending_sequences"] == []


def test_fork_on_unknown_source_returns_404(client):
    r = _fork(client, "no-such-source", "g", "f-404-dst", 0)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"
    # No target stream was created.
    assert client.get("/streams/f-404-dst").status_code == 404


def test_fork_into_existing_target_returns_409(client):
    _seed_for_fork(client, "f-src-taken")
    _post(client, "f-taken", "e0", 0, 1)
    r = _fork(client, "f-src-taken", "g", "f-taken", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "target_stream_exists"


def test_fork_through_sequence_out_of_range_returns_422(client):
    _seed_for_fork(client, "f-src-range")
    r = _fork(client, "f-src-range", "g-too-far", "f-too-far", 6)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "through_sequence_out_of_range"
    # The failed fork left nothing behind.
    assert client.get("/streams/f-too-far").status_code == 404


def test_fork_retry_with_same_parameters_is_idempotent(client):
    _seed_for_fork(client, "f-src-retry1")
    first = _fork(client, "f-src-retry1", "retry-fork", "f-retry", 4)
    assert first.status_code == 201

    second = _fork(client, "f-src-retry1", "retry-fork", "f-retry", 4)
    assert second.status_code == 200
    assert second.json() == first.json()

    # Exactly one copy of each event exists in the target.
    claim = _claim(client, "f-retry", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1, 2, 3]


def test_fork_retry_with_different_parameters_returns_409(client):
    _seed_for_fork(client, "f-src-retry2")
    assert _fork(client, "f-src-retry2", "retry-fork", "f-retry2", 4).status_code == 201

    r = _fork(client, "f-src-retry2", "retry-fork", "f-retry2", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_replay_conflict"

    r = _fork(client, "f-src-retry2", "retry-fork", "f-retry2-other", 4)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "fork_replay_conflict"

    # No extra streams/events were created by the clashing retries.
    assert client.get("/streams/f-retry2-other").status_code == 404


def test_fork_validation_rejects_bad_payloads(client):
    _seed_for_fork(client, "f-src-val")
    url = "/streams/f-src-val/forks"
    assert client.post(url, json={"fork_id": "", "target_stream_id": "t", "through_sequence": 0}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "", "through_sequence": 0}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t", "through_sequence": -1}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t", "through_sequence": True}).status_code == 422
    assert client.post(url, json={"fork_id": "g", "target_stream_id": "t"}).status_code == 422


def test_concurrent_forks_one_idempotent_fork(client):
    import threading

    _seed_for_fork(client, "f-src-conc1")
    statuses: list[int] = []

    def do_fork():
        r = _fork(client, "f-src-conc1", "concurrent-fork", "f-concurrent", 3)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_fork) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    state = client.get("/streams/f-concurrent").json()
    assert state["next_sequence"] == 3 and state["balance"] == 90
    claim = _claim(client, "f-concurrent", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1, 2]


def test_concurrent_forks_distinct_ids_same_target_never_half_build(client):
    import threading

    _seed_for_fork(client, "f-src-conc2")
    codes: list[str] = []

    def do_fork(i):
        r = _fork(client, "f-src-conc2", f"race-fork-{i}", "f-race2", 2)
        if r.status_code != 201:
            codes.append(r.json()["detail"]["code"])

    threads = [threading.Thread(target=do_fork, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(codes) == 19
    assert set(codes) == {"target_stream_exists"}
    state = client.get("/streams/f-race2").json()
    assert state["balance"] == 70 and state["next_sequence"] == 2
    claim = _claim(client, "f-race2", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1]


# --------------------------------------------------------------------------- #
# Cross-stream atomic transfers
# --------------------------------------------------------------------------- #


def _transfer(client, transfer_id, source, target, amount):
    return client.post(
        "/transfers",
        json={
            "transfer_id": transfer_id,
            "source_stream_id": source,
            "target_stream_id": target,
            "amount": amount,
        },
    )


def _seed_balance(client, stream_id, amount):
    """Create a stream whose sole applied event credits ``amount`` at sequence 0."""
    r = _post(client, stream_id, f"{stream_id}-seed", 0, amount)
    assert r.status_code in (200, 201)


def test_transfer_moves_amount_and_reports_both_sides(client):
    _seed_balance(client, "t-a", 100)
    _seed_balance(client, "t-b", 20)

    r = _transfer(client, "txn-1", "t-a", "t-b", 30)
    assert r.status_code == 201
    body = r.json()
    assert body["replayed"] is False
    assert body["amount"] == 30
    assert body["source"] == {
        "stream_id": "t-a",
        "event_id": "transfer:txn-1:debit",
        "sequence": 1,
        "state": {
            "stream_id": "t-a",
            "balance": 70,
            "next_sequence": 2,
            "pending_sequences": [],
        },
    }
    assert body["target"] == {
        "stream_id": "t-b",
        "event_id": "transfer:txn-1:credit",
        "sequence": 1,
        "state": {
            "stream_id": "t-b",
            "balance": 50,
            "next_sequence": 2,
            "pending_sequences": [],
        },
    }

    # The synthetic events are visible through the ordinary stream endpoint.
    assert client.get("/streams/t-a").json()["balance"] == 70
    assert client.get("/streams/t-b").json()["balance"] == 50


def test_transfer_events_are_consumable_like_normal_events(client):
    _seed_balance(client, "t-consume-a", 100)
    _seed_balance(client, "t-consume-b", 0)
    r = _transfer(client, "txn-consume", "t-consume-a", "t-consume-b", 40)
    assert r.status_code == 201

    batch_a = _claim(client, "t-consume-a", "worker", 100).json()
    assert [(e["sequence"], e["delta"]) for e in batch_a["events"]] == [(0, 100), (1, -40)]
    batch_b = _claim(client, "t-consume-b", "worker", 100).json()
    assert [(e["sequence"], e["delta"]) for e in batch_b["events"]] == [(0, 0), (1, 40)]


def test_transfer_exact_balance_succeeds(client):
    _seed_balance(client, "t-exact", 50)
    _seed_balance(client, "t-exact-dst", 0)
    r = _transfer(client, "txn-exact", "t-exact", "t-exact-dst", 50)
    assert r.status_code == 201
    assert r.json()["source"]["state"]["balance"] == 0
    assert r.json()["target"]["state"]["balance"] == 50


def test_transfer_insufficient_funds_returns_422_and_changes_nothing(client):
    _seed_balance(client, "t-poor", 10)
    _seed_balance(client, "t-poor-dst", 0)
    r = _transfer(client, "txn-poor", "t-poor", "t-poor-dst", 11)
    assert r.status_code == 422
    assert r.json()["detail"]["code"] == "insufficient_funds"

    # No events, no balance movement, no transfer record.
    assert client.get("/streams/t-poor").json() == {
        "stream_id": "t-poor",
        "balance": 10,
        "next_sequence": 1,
        "pending_sequences": [],
    }
    assert client.get("/streams/t-poor-dst").json()["balance"] == 0
    # The same transfer_id is still usable after the failed attempt.
    assert _transfer(client, "txn-poor", "t-poor", "t-poor-dst", 5).status_code == 201


def test_transfer_missing_stream_returns_404_and_creates_nothing(client):
    _seed_balance(client, "t-existing", 10)
    r = _transfer(client, "txn-404-a", "t-missing-src", "t-existing", 1)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "source_stream_not_found"
    assert client.get("/streams/t-missing-src").status_code == 404

    r = _transfer(client, "txn-404-b", "t-existing", "t-missing-dst", 1)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "target_stream_not_found"
    assert client.get("/streams/t-missing-dst").status_code == 404
    # Nothing was debited from the existing stream.
    assert client.get("/streams/t-existing").json()["balance"] == 10


def test_transfer_replay_with_same_parameters_returns_original_result(client):
    _seed_balance(client, "t-rep-a", 100)
    _seed_balance(client, "t-rep-b", 0)
    first = _transfer(client, "txn-rep", "t-rep-a", "t-rep-b", 30)
    assert first.status_code == 201
    first_body = first.json()

    second = _transfer(client, "txn-rep", "t-rep-a", "t-rep-b", 30)
    assert second.status_code == 200
    second_body = second.json()
    assert second_body["replayed"] is True
    # Original event coordinates are preserved ...
    assert (
        second_body["source"]["sequence"] == first_body["source"]["sequence"] == 1
    )
    assert (
        second_body["target"]["sequence"] == first_body["target"]["sequence"] == 1
    )
    assert second_body["source"]["event_id"] == "transfer:txn-rep:debit"
    assert second_body["target"]["event_id"] == "transfer:txn-rep:credit"

    # ... and the source was not debited a second time.
    assert client.get("/streams/t-rep-a").json()["balance"] == 70
    assert client.get("/streams/t-rep-b").json()["balance"] == 30
    claim = _claim(client, "t-rep-a", "worker", 100).json()
    assert [e["sequence"] for e in claim["events"]] == [0, 1]


def test_transfer_replay_after_other_events_reports_current_state(client):
    _seed_balance(client, "t-late-a", 100)
    _seed_balance(client, "t-late-b", 0)
    first = _transfer(client, "txn-late", "t-late-a", "t-late-b", 30).json()

    # More events land on both streams; the replay keeps the original event
    # sequences but returns the latest stream state.
    _post(client, "t-late-a", "a-extra", 2, -5)
    _post(client, "t-late-b", "b-extra", 2, 7)

    second = _transfer(client, "txn-late", "t-late-a", "t-late-b", 30).json()
    assert second["source"]["sequence"] == first["source"]["sequence"] == 1
    assert second["target"]["sequence"] == first["target"]["sequence"] == 1
    assert second["source"]["state"]["balance"] == 65
    assert second["source"]["state"]["next_sequence"] == 3
    assert second["target"]["state"]["balance"] == 37
    assert second["target"]["state"]["next_sequence"] == 3


def test_transfer_replay_with_different_parameters_returns_409(client):
    _seed_balance(client, "t-conf-a", 100)
    _seed_balance(client, "t-conf-b", 100)
    _seed_balance(client, "t-conf-c", 100)
    assert _transfer(client, "txn-conf", "t-conf-a", "t-conf-b", 10).status_code == 201

    r = _transfer(client, "txn-conf", "t-conf-a", "t-conf-b", 20)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "txn-conf", "t-conf-c", "t-conf-b", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    r = _transfer(client, "txn-conf", "t-conf-a", "t-conf-c", 10)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "transfer_replay_conflict"

    # The clashing retries moved no extra money.
    assert client.get("/streams/t-conf-a").json()["balance"] == 90
    assert client.get("/streams/t-conf-b").json()["balance"] == 110
    assert client.get("/streams/t-conf-c").json()["balance"] == 100


def test_concurrent_transfer_retries_commit_exactly_once(client):
    import threading

    _seed_balance(client, "t-race-a", 100)
    _seed_balance(client, "t-race-b", 0)
    statuses: list[int] = []

    def do_transfer():
        r = _transfer(client, "txn-race", "t-race-a", "t-race-b", 40)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_transfer) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    assert client.get("/streams/t-race-a").json()["balance"] == 60
    assert client.get("/streams/t-race-b").json()["balance"] == 40
    claim_a = _claim(client, "t-race-a", "worker", 100).json()
    claim_b = _claim(client, "t-race-b", "worker", 100).json()
    assert [e["sequence"] for e in claim_a["events"]] == [0, 1]
    assert [e["sequence"] for e in claim_b["events"]] == [0, 1]


def test_concurrent_distinct_transfers_serialise_and_conserve_money(client):
    import threading

    _seed_balance(client, "t-many-a", 100)
    _seed_balance(client, "t-many-b", 100)

    def do_transfer(i):
        _transfer(client, f"txn-many-{i}", "t-many-a", "t-many-b", 1)

    threads = [threading.Thread(target=do_transfer, args=(i,)) for i in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Every one-unit transfer landed exactly once; total money is conserved.
    state_a = client.get("/streams/t-many-a").json()
    state_b = client.get("/streams/t-many-b").json()
    assert state_a["balance"] == 50 and state_a["next_sequence"] == 51
    assert state_b["balance"] == 150 and state_b["next_sequence"] == 51
    assert state_a["balance"] + state_b["balance"] == 200


def test_transfer_unblocks_buffered_events_on_both_sides(client):
    # Source: sequence 1 is parked behind a gap at 0; the transfer lands at
    # next_sequence 0 and must unblock the buffered event via contiguous catch-up.
    _post(client, "t-gap-a", "a-e1", 1, -30)  # buffered, balance 0, next 0
    _post(client, "t-gap-b", "b-e1", 1, 5)  # buffered, balance 0, next 0
    # Seed applied prefixes so both streams exist with spendable balances.
    _post(client, "t-gap-a", "a-e0", 0, 100)  # catch-up -> balance 70, next 2
    _post(client, "t-gap-b", "b-e0", 0, 0)  # catch-up -> balance 5, next 2
    # Park another buffered run behind sequence 3 on both sides.
    _post(client, "t-gap-a", "a-e4", 4, 2)
    _post(client, "t-gap-b", "b-e4", 4, 1)

    r = _transfer(client, "txn-gap", "t-gap-a", "t-gap-b", 20)
    assert r.status_code == 201
    body = r.json()
    # Transfer events appended at the current next_sequence (2); sequence 3 is
    # still a gap on both sides, so [4] stays buffered.
    assert body["source"]["sequence"] == 2
    assert body["target"]["sequence"] == 2
    assert body["source"]["state"] == {
        "stream_id": "t-gap-a",
        "balance": 50,  # 70 - 20
        "next_sequence": 3,
        "pending_sequences": [4],
    }
    assert body["target"]["state"] == {
        "stream_id": "t-gap-b",
        "balance": 25,  # 5 + 20
        "next_sequence": 3,
        "pending_sequences": [4],
    }

    # Closing the gap applies the still-buffered event normally.
    _post(client, "t-gap-a", "a-e3", 3, 1)
    _post(client, "t-gap-b", "b-e3", 3, 1)
    assert client.get("/streams/t-gap-a").json()["balance"] == 53
    assert client.get("/streams/t-gap-b").json()["balance"] == 27


def test_transfer_validation_rejects_bad_payloads(client):
    _seed_balance(client, "t-val", 10)
    base = {"transfer_id": "g", "source_stream_id": "t-val", "target_stream_id": "x", "amount": 1}

    r = client.post("/transfers", json={**base, "transfer_id": ""})
    assert r.status_code == 422
    r = client.post("/transfers", json={**base, "source_stream_id": ""})
    assert r.status_code == 422
    r = client.post("/transfers", json={**base, "target_stream_id": ""})
    assert r.status_code == 422
    for bad_amount in (0, -1, "5", True):
        assert client.post("/transfers", json={**base, "amount": bad_amount}).status_code == 422
    # Source and target must be different streams.
    r = client.post(
        "/transfers",
        json={**base, "target_stream_id": "t-val"},
    )
    assert r.status_code == 422
    # Missing fields.
    assert client.post("/transfers", json={"transfer_id": "g"}).status_code == 422
    assert client.post("/transfers", json={}).status_code == 422


def test_transfer_coexists_with_forks_and_consumers(client):
    _seed_balance(client, "t-mix-a", 100)
    _seed_balance(client, "t-mix-b", 0)
    assert _transfer(client, "txn-mix-2", "t-mix-a", "t-mix-b", 40).status_code == 201

    # Fork the source: both seed (0) and debit (1) events land in the copy.
    r = _fork(client, "t-mix-a", "fork-mix", "t-mix-fork", 2)
    assert r.status_code == 201
    assert r.json()["balance"] == 60 and r.json()["next_sequence"] == 2

    # Consumers see transfer events in order, and ack still works.
    batch = _claim(client, "t-mix-a", "worker", 10).json()
    assert [(e["event_id"], e["sequence"], e["delta"]) for e in batch["events"]] == [
        ("t-mix-a-seed", 0, 100),
        ("transfer:txn-mix-2:debit", 1, -40),
    ]
    assert _ack(client, "t-mix-a", "worker", batch["lease_token"]).json()["cursor"] == 2
    assert _claim(client, "t-mix-a", "worker", 10).json()["events"] == []


# --------------------------------------------------------------------------- #
# Event reversals
# --------------------------------------------------------------------------- #


def _reverse(client, stream_id, event_id, reversal_id):
    return client.post(
        f"/streams/{stream_id}/events/{event_id}/reversals",
        json={"reversal_id": reversal_id},
    )


def _seed_deltas(client, stream_id, deltas):
    for seq, delta in enumerate(deltas):
        r = _post(client, stream_id, f"{stream_id}-e{seq}", seq, delta)
        assert r.status_code in (200, 201)


def test_reversal_appends_compensation_event(client):
    _seed_deltas(client, "r-basic", (100, -30, 20))  # balance 90, next_sequence 3

    r = _reverse(client, "r-basic", "r-basic-e2", "rev-1")
    assert r.status_code == 201
    body = r.json()
    assert body["replayed"] is False
    assert body["reversal_id"] == "rev-1"
    assert body["stream_id"] == "r-basic"
    assert body["event_id"] == "r-basic-e2"
    assert body["reversal_event_id"] == "reversal:rev-1"
    assert body["sequence"] == 3
    assert body["delta"] == -20
    assert body["stream"] == {
        "stream_id": "r-basic",
        "balance": 70,
        "next_sequence": 4,
        "pending_sequences": [],
    }

    # Reversing a negative-delta event credits the stream back.
    r = _reverse(client, "r-basic", "r-basic-e1", "rev-2")
    assert r.status_code == 201
    body = r.json()
    assert body["delta"] == 30
    assert body["sequence"] == 4
    assert body["stream"]["balance"] == 100
    assert body["stream"]["next_sequence"] == 5

    # The compensation events are ordinary applied events on the stream.
    state = client.get("/streams/r-basic").json()
    assert state["balance"] == 100 and state["next_sequence"] == 5


def test_reversal_replay_returns_200_with_latest_state(client):
    _seed_deltas(client, "r-rep", (50, 50))  # balance 100, next_sequence 2
    first = _reverse(client, "r-rep", "r-rep-e0", "rev-rep")
    assert first.status_code == 201
    assert first.json()["stream"]["balance"] == 50

    # More events land after the reversal.
    _post(client, "r-rep", "r-rep-e3", 3, 7)

    second = _reverse(client, "r-rep", "r-rep-e0", "rev-rep")
    assert second.status_code == 200
    body = second.json()
    assert body["replayed"] is True
    # Original event coordinates are preserved ...
    assert body["sequence"] == first.json()["sequence"] == 2
    assert body["delta"] == -50
    assert body["reversal_event_id"] == "reversal:rev-rep"
    # ... the state is the latest, and the compensation happened exactly once.
    assert body["stream"] == {
        "stream_id": "r-rep",
        "balance": 57,
        "next_sequence": 4,
        "pending_sequences": [],
    }
    assert client.get("/streams/r-rep").json()["balance"] == 57


def test_reversal_same_id_different_target_returns_409(client):
    _seed_deltas(client, "r-drift-a", (10, 20))
    _seed_deltas(client, "r-drift-b", (5,))
    assert _reverse(client, "r-drift-a", "r-drift-a-e0", "rev-drift").status_code == 201

    # Same reversal_id, different event.
    r = _reverse(client, "r-drift-a", "r-drift-a-e1", "rev-drift")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "reversal_replay_conflict"

    # Same reversal_id, different stream.
    r = _reverse(client, "r-drift-b", "r-drift-b-e0", "rev-drift")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "reversal_replay_conflict"

    # The clashing retries compensated nothing extra.
    assert client.get("/streams/r-drift-a").json()["balance"] == 20
    assert client.get("/streams/r-drift-b").json()["balance"] == 5


def test_reversal_same_event_twice_returns_409(client):
    _seed_deltas(client, "r-twice", (40,))
    assert _reverse(client, "r-twice", "r-twice-e0", "rev-a").status_code == 201

    r = _reverse(client, "r-twice", "r-twice-e0", "rev-b")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "event_already_reversed"

    # No second compensation event was appended ...
    state = client.get("/streams/r-twice").json()
    assert state["balance"] == 0 and state["next_sequence"] == 2

    # ... and the rejected reversal_id was not burned by the failed attempt.
    _seed_deltas(client, "r-twice-b", (9,))
    assert _reverse(client, "r-twice-b", "r-twice-b-e0", "rev-b").status_code == 201


def test_reversal_unknown_stream_or_event_returns_404(client):
    r = _reverse(client, "r-ghost", "e0", "rev-404-a")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    _seed_deltas(client, "r-404", (10,))
    r = _reverse(client, "r-404", "no-such-event", "rev-404-b")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "event_not_found"

    # Nothing was written; the stream and the reversal_id remain usable.
    assert client.get("/streams/r-404").json()["balance"] == 10
    assert _reverse(client, "r-404", "r-404-e0", "rev-404-b").status_code == 201


def test_reversal_of_unapplied_event_returns_409(client):
    _post(client, "r-pend", "r-pend-e1", 1, 30)  # buffered behind a gap at 0
    r = _reverse(client, "r-pend", "r-pend-e1", "rev-pend")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "event_not_applied"

    # Once the gap closes and the event is applied, the reversal succeeds.
    _post(client, "r-pend", "r-pend-e0", 0, 100)
    r = _reverse(client, "r-pend", "r-pend-e1", "rev-pend")
    assert r.status_code == 201
    assert r.json()["sequence"] == 2
    assert r.json()["stream"]["balance"] == 100  # 100 + 30 - 30
    assert r.json()["stream"]["next_sequence"] == 3


def test_reversal_validation_rejects_bad_payloads(client):
    _seed_deltas(client, "r-val", (10,))
    url = "/streams/r-val/events/r-val-e0/reversals"
    assert client.post(url, json={}).status_code == 422
    assert client.post(url, json={"reversal_id": ""}).status_code == 422
    assert client.post(url, json={"reversal_id": "   "}).status_code == 422
    assert client.post(url, json={"reversal_id": 123}).status_code == 422
    assert client.post(url, json={"reversal_id": True}).status_code == 422


def test_reversal_unblocks_buffered_events(client):
    _seed_deltas(client, "r-gap", (10, 20))  # applied 0,1; balance 30, next 2
    _post(client, "r-gap", "r-gap-e3", 3, 5)  # buffered behind a gap at 2

    r = _reverse(client, "r-gap", "r-gap-e0", "rev-gap")
    assert r.status_code == 201
    body = r.json()
    assert body["sequence"] == 2
    # The compensation event closed the gap: the buffered event applied too.
    assert body["stream"] == {
        "stream_id": "r-gap",
        "balance": 25,  # 30 - 10 + 5
        "next_sequence": 4,
        "pending_sequences": [],
    }


def test_reversal_event_is_consumable_and_forkable(client):
    _seed_deltas(client, "r-obs", (100, 20))  # balance 120, next_sequence 2
    assert _reverse(client, "r-obs", "r-obs-e1", "rev-obs").status_code == 201

    # Consumers see the compensation event like any other applied event.
    batch = _claim(client, "r-obs", "worker", 10).json()
    assert [(e["event_id"], e["sequence"], e["delta"]) for e in batch["events"]] == [
        ("r-obs-e0", 0, 100),
        ("r-obs-e1", 1, 20),
        ("reversal:rev-obs", 2, -20),
    ]
    assert _ack(client, "r-obs", "worker", batch["lease_token"]).json()["cursor"] == 3

    # Forks copy the compensation event as part of the applied prefix.
    r = _fork(client, "r-obs", "fork-obs", "r-obs-fork", 3)
    assert r.status_code == 201
    assert r.json()["balance"] == 100 and r.json()["next_sequence"] == 3
    forked = _claim(client, "r-obs-fork", "worker", 10).json()
    assert [e["event_id"] for e in forked["events"]] == [
        "r-obs-e0",
        "r-obs-e1",
        "reversal:rev-obs",
    ]


def test_reversal_of_transfer_event(client):
    _seed_balance(client, "r-t-a", 100)
    _seed_balance(client, "r-t-b", 0)
    assert _transfer(client, "txn-rev", "r-t-a", "r-t-b", 40).status_code == 201

    # Undo the debit side: the compensation credits the source back.
    r = _reverse(client, "r-t-a", "transfer:txn-rev:debit", "rev-txn")
    assert r.status_code == 201
    body = r.json()
    assert body["delta"] == 40
    assert body["stream"]["balance"] == 100
    assert body["stream"]["next_sequence"] == 3


def test_reversal_of_a_reversal_event(client):
    _seed_deltas(client, "r-chain", (50,))
    assert _reverse(client, "r-chain", "r-chain-e0", "rev-chain-1").status_code == 201
    assert client.get("/streams/r-chain").json()["balance"] == 0

    # The compensation event is itself an ordinary applied event and can be
    # reversed (undoing the undo).
    r = _reverse(client, "r-chain", "reversal:rev-chain-1", "rev-chain-2")
    assert r.status_code == 201
    assert r.json()["delta"] == 50
    assert client.get("/streams/r-chain").json()["balance"] == 50


def test_concurrent_reversal_retries_commit_exactly_once(client):
    import threading

    _seed_deltas(client, "r-race", (80,))
    statuses: list[int] = []

    def do_reverse():
        r = _reverse(client, "r-race", "r-race-e0", "rev-race")
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_reverse) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    state = client.get("/streams/r-race").json()
    assert state["balance"] == 0 and state["next_sequence"] == 2
    # Exactly one compensation event exists.
    batch = _claim(client, "r-race", "worker", 10).json()
    assert [e["event_id"] for e in batch["events"]] == [
        "r-race-e0",
        "reversal:rev-race",
    ]


def test_concurrent_distinct_reversals_same_event_compensate_once(client):
    import threading

    _seed_deltas(client, "r-race2", (80,))
    codes: list[str] = []
    successes: list[int] = []

    def do_reverse(i):
        r = _reverse(client, "r-race2", "r-race2-e0", f"rev-race2-{i}")
        if r.status_code == 201:
            successes.append(i)
        else:
            codes.append(r.json()["detail"]["code"])

    threads = [threading.Thread(target=do_reverse, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(successes) == 1
    assert len(codes) == 19 and set(codes) == {"event_already_reversed"}
    state = client.get("/streams/r-race2").json()
    assert state["balance"] == 0 and state["next_sequence"] == 2


# --------------------------------------------------------------------------- #
# Stream sealing
# --------------------------------------------------------------------------- #


def _seal(client, stream_id, seal_id, expected_next_sequence):
    return client.post(
        f"/streams/{stream_id}/seal",
        json={
            "seal_id": seal_id,
            "expected_next_sequence": expected_next_sequence,
        },
    )


def _expected_sha256(events):
    import hashlib
    import json

    lines = [
        json.dumps([seq, event_id, delta], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        for seq, event_id, delta in events
    ]
    return hashlib.sha256(b"\n".join(lines)).hexdigest()


def test_seal_creates_record_with_canonical_sha256(client):
    events = [("z-e0", 0, 100), ("z-e1", 1, -30), ("z-e2", 2, 20)]
    for event_id, seq, delta in events:
        _post(client, "z-basic", event_id, seq, delta)

    r = _seal(client, "z-basic", "seal-1", 3)
    assert r.status_code == 201
    body = r.json()
    assert body == {
        "status": "ok",
        "replayed": False,
        "stream_id": "z-basic",
        "seal_id": "seal-1",
        "next_sequence": 3,
        "event_count": 3,
        "sha256": _expected_sha256([(s, e, d) for e, s, d in events]),
    }


def test_seal_preserves_unicode_event_ids_without_ascii_escaping(client):
    import hashlib
    import json

    # Non-ASCII characters stay literal in the canonical JSON (ensure_ascii=False).
    _post(client, "z-uni", "事件-0", 0, 5)
    r = _seal(client, "z-uni", "seal-uni", 1)
    assert r.status_code == 201
    line = json.dumps([0, "事件-0", 5], ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    assert r.json()["sha256"] == hashlib.sha256(line).hexdigest()


def test_seal_empty_stream_hashes_empty_input(client):
    # A forked-through-0 stream exists but contains no events.
    _post(client, "z-empty-src", "e0", 0, 1)
    _fork(client, "z-empty-src", "f", "z-empty", 0)

    r = _seal(client, "z-empty", "seal-empty", 0)
    assert r.status_code == 201
    assert r.json()["event_count"] == 0
    assert r.json()["sha256"] == _expected_sha256([])


def test_seal_unknown_stream_returns_404(client):
    r = _seal(client, "z-ghost", "seal-x", 0)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_seal_with_stale_version_returns_409_and_writes_nothing(client):
    _post(client, "z-ver", "e0", 0, 10)
    _post(client, "z-ver", "e1", 1, 20)

    r = _seal(client, "z-ver", "seal-ver", 1)  # actual next_sequence is 2
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "version_mismatch"

    # The failed seal did not burn the seal_id; sealing at the right version works.
    r = _seal(client, "z-ver", "seal-ver", 2)
    assert r.status_code == 201


def test_seal_with_pending_buffered_events_returns_409(client):
    _post(client, "z-pend", "e1", 1, 10)  # buffered behind gap at 0
    r = _seal(client, "z-pend", "seal-pend", 0)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stream_has_pending_events"

    # Close the gap: the same request now seals.
    _post(client, "z-pend", "e0", 0, 5)
    r = _seal(client, "z-pend", "seal-pend", 2)
    assert r.status_code == 201
    assert r.json()["event_count"] == 2


def test_seal_retry_with_same_parameters_is_idempotent(client):
    _post(client, "z-rep", "e0", 0, 10)
    first = _seal(client, "z-rep", "seal-rep", 1)
    assert first.status_code == 201

    second = _seal(client, "z-rep", "seal-rep", 1)
    assert second.status_code == 200
    body = second.json()
    assert body["replayed"] is True
    assert body["sha256"] == first.json()["sha256"]


def test_seal_retry_with_drifted_parameters_returns_409(client):
    _post(client, "z-drift", "e0", 0, 10)
    assert _seal(client, "z-drift", "seal-a", 1).status_code == 201

    r = _seal(client, "z-drift", "seal-b", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "seal_replay_conflict"

    r = _seal(client, "z-drift", "seal-a", 2)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "seal_replay_conflict"


def test_sealed_stream_rejects_new_submission_but_replays_existing_event(client):
    _post(client, "z-seal-sub", "e0", 0, 10)
    assert _seal(client, "z-seal-sub", "seal-1", 1).status_code == 201

    # A genuinely new event is rejected.
    r = _post(client, "z-seal-sub", "e1", 1, 20)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stream_sealed"
    assert client.get("/streams/z-seal-sub").json()["next_sequence"] == 1

    # The pre-seal event's idempotent replay still returns the original result.
    r = _post(client, "z-seal-sub", "e0", 0, 10)
    assert r.status_code == 200
    assert r.json()["replayed"] is True
    assert client.get("/streams/z-seal-sub").json()["balance"] == 10


def test_sealed_stream_rejects_transfers_on_either_side_but_replays(client):
    _seed_balance(client, "z-t-a", 100)
    _seed_balance(client, "z-t-b", 0)
    _seed_balance(client, "z-t-c", 0)
    # A completed transfer before sealing.
    assert _transfer(client, "z-txn-1", "z-t-a", "z-t-b", 30).status_code == 201
    assert _seal(client, "z-t-a", "seal-a", 2).status_code == 201
    assert _seal(client, "z-t-b", "seal-b", 2).status_code == 201

    # New transfers touching a sealed stream on either side are rejected.
    r = _transfer(client, "z-txn-new-a", "z-t-a", "z-t-c", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stream_sealed"
    r = _transfer(client, "z-txn-new-b", "z-t-c", "z-t-b", 1)
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stream_sealed"

    # Balances are untouched by the rejected transfers.
    assert client.get("/streams/z-t-a").json()["balance"] == 70
    assert client.get("/streams/z-t-b").json()["balance"] == 30
    assert client.get("/streams/z-t-c").json()["balance"] == 0

    # The pre-seal transfer still replays idempotently with original coordinates.
    r = _transfer(client, "z-txn-1", "z-t-a", "z-t-b", 30)
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] is True
    assert body["source"]["sequence"] == 1
    assert body["target"]["sequence"] == 1


def test_sealed_stream_rejects_new_reversal_but_replays_existing(client):
    _seed_deltas(client, "z-r", (100, -30, 20))  # balance 90, next 3
    assert _reverse(client, "z-r", "z-r-e2", "z-rev-1").status_code == 201  # balance 70, next 4
    assert _seal(client, "z-r", "seal-r", 4).status_code == 201

    # A new reversal is rejected.
    r = _reverse(client, "z-r", "z-r-e1", "z-rev-2")
    assert r.status_code == 409
    assert r.json()["detail"]["code"] == "stream_sealed"
    assert client.get("/streams/z-r").json()["balance"] == 70

    # The pre-seal reversal still replays with its original coordinates.
    r = _reverse(client, "z-r", "z-r-e2", "z-rev-1")
    assert r.status_code == 200
    body = r.json()
    assert body["replayed"] is True
    assert body["sequence"] == 3 and body["delta"] == -20


def test_sealed_stream_remains_queryable_consumable_and_forkable(client):
    _seed_deltas(client, "z-ro", (10, 20, 30))  # balance 60, next 3
    seal = _seal(client, "z-ro", "seal-ro", 3)
    assert seal.status_code == 201

    # Query.
    assert client.get("/streams/z-ro").json() == {
        "stream_id": "z-ro",
        "balance": 60,
        "next_sequence": 3,
        "pending_sequences": [],
    }

    # Claim/ack consumption works normally.
    batch = _claim(client, "z-ro", "worker", 10).json()
    assert [e["sequence"] for e in batch["events"]] == [0, 1, 2]
    assert _ack(client, "z-ro", "worker", batch["lease_token"]).json()["cursor"] == 3

    # The sealed stream can be a fork source; the fork target stays appendable.
    r = _fork(client, "z-ro", "fork-ro", "z-ro-copy", 3)
    assert r.status_code == 201
    assert r.json()["balance"] == 60 and r.json()["next_sequence"] == 3
    r = _post(client, "z-ro-copy", "z-ro-copy-e3", 3, 5)
    assert r.status_code == 201
    assert r.json()["stream"]["balance"] == 65


def test_seal_validation_rejects_bad_payloads(client):
    _post(client, "z-val", "e0", 0, 1)
    url = "/streams/z-val/seal"
    assert client.post(url, json={"seal_id": "", "expected_next_sequence": 1}).status_code == 422
    assert client.post(url, json={"seal_id": "s", "expected_next_sequence": -1}).status_code == 422
    assert client.post(url, json={"seal_id": "s", "expected_next_sequence": True}).status_code == 422
    assert client.post(url, json={"seal_id": "s"}).status_code == 422
    assert client.post(url, json={"expected_next_sequence": 1}).status_code == 422


def test_concurrent_seal_retries_commit_exactly_once(client):
    import threading

    _seed_deltas(client, "z-conc", (1, 2, 3))
    statuses: list[int] = []

    def do_seal():
        r = _seal(client, "z-conc", "seal-conc", 3)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=do_seal) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(statuses) == [200] * 19 + [201]
    # The stream stayed sealed and never accepted another event.
    assert client.get("/streams/z-conc").json()["next_sequence"] == 3


# --------------------------------------------------------------------------- #
# Event history snapshot export
# --------------------------------------------------------------------------- #


def _page(client, stream_id, limit=None, cursor=None):
    params = {}
    if limit is not None:
        params["limit"] = limit
    if cursor is not None:
        params["cursor"] = cursor
    return client.get(f"/streams/{stream_id}/events", params=params)


def _walk_pages(client, stream_id, limit):
    """Follow next_cursor to the end; return (events, pages, last_body)."""
    events, cursor, pages, body = [], None, 0, None
    while True:
        r = _page(client, stream_id, limit=limit, cursor=cursor)
        assert r.status_code == 200
        body = r.json()
        events.extend(body["events"])
        pages += 1
        cursor = body["next_cursor"]
        if cursor is None:
            return events, pages, body


def _sign_payload(payload_bytes):
    import base64
    import hashlib
    import hmac

    from app import config

    body = base64.urlsafe_b64encode(payload_bytes).decode("ascii")
    sig = base64.urlsafe_b64encode(
        hmac.new(
            config.snapshot_cursor_secret().encode("utf-8"),
            payload_bytes,
            hashlib.sha256,
        ).digest()
    ).decode("ascii")
    return f"{body}.{sig}"


def _craft_cursor(payload_dict):
    import json

    return _sign_payload(
        json.dumps(
            payload_dict, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")
    )


def test_snapshot_export_paginates_in_sequence_order(client):
    _seed_deltas(client, "x-basic", (100, -30, 20, 10, 5))  # balance 105, next 5

    r = _page(client, "x-basic", limit=2)
    assert r.status_code == 200
    body = r.json()
    assert body["stream_id"] == "x-basic"
    assert body["snapshot_next_sequence"] == 5
    assert body["balance"] == 105
    assert [e["sequence"] for e in body["events"]] == [0, 1]
    assert body["next_cursor"]

    events, pages, last = _walk_pages(client, "x-basic", 2)
    assert pages == 3
    assert [e["sequence"] for e in events] == [0, 1, 2, 3, 4]
    assert [e["delta"] for e in events] == [100, -30, 20, 10, 5]
    assert [e["event_id"] for e in events] == [f"x-basic-e{i}" for i in range(5)]
    # The last page terminates the export.
    assert last["next_cursor"] is None
    # Every page reports the same pinned snapshot bound and version balance.
    assert last["snapshot_next_sequence"] == 5
    assert last["balance"] == 105


def test_snapshot_export_exact_page_boundary_ends_cleanly(client):
    _seed_deltas(client, "x-even", (1, 2, 3, 4))  # exactly two pages of 2

    events, pages, last = _walk_pages(client, "x-even", 2)
    assert pages == 2
    assert [e["sequence"] for e in events] == [0, 1, 2, 3]
    assert last["events"][1]["sequence"] == 3
    assert last["next_cursor"] is None


def test_snapshot_is_stable_under_concurrent_appends(client):
    _seed_deltas(client, "x-conc", (10, 20, 30, 40, 50))  # balance 150, next 5

    first = _page(client, "x-conc", limit=2).json()
    assert first["snapshot_next_sequence"] == 5
    cursor = first["next_cursor"]

    # Concurrent appends land after the snapshot was pinned.
    _post(client, "x-conc", "x-conc-e5", 5, 60)
    _post(client, "x-conc", "x-conc-e6", 6, 70)
    assert client.get("/streams/x-conc").json()["next_sequence"] == 7

    # The ongoing export keeps reading the pinned snapshot: no new events,
    # no gaps, no duplicates, and the balance stays at the snapshot version.
    events, pages, last = [first["events"]], 1, None
    while cursor is not None:
        body = _page(client, "x-conc", limit=2, cursor=cursor).json()
        events.append(body["events"])
        cursor = body["next_cursor"]
        pages += 1
        last = body
    flat = [e for page in events for e in page]
    assert pages == 3
    assert [e["sequence"] for e in flat] == [0, 1, 2, 3, 4]
    assert last["snapshot_next_sequence"] == 5
    assert last["balance"] == 150

    # A fresh export pins a new snapshot that includes the appended events.
    fresh = _page(client, "x-conc", limit=100).json()
    assert fresh["snapshot_next_sequence"] == 7
    assert fresh["balance"] == 280
    assert [e["sequence"] for e in fresh["events"]] == list(range(7))


def test_snapshot_export_reuses_old_cursor_for_same_page(client):
    _seed_deltas(client, "x-replay", (1, 2, 3, 4))

    first = _page(client, "x-replay", limit=2).json()
    _post(client, "x-replay", "x-replay-e4", 4, 5)  # appended after the snapshot

    page2a = _page(client, "x-replay", limit=2, cursor=first["next_cursor"]).json()
    page2b = _page(client, "x-replay", limit=2, cursor=first["next_cursor"]).json()
    assert page2a == page2b
    assert [e["sequence"] for e in page2a["events"]] == [2, 3]
    assert page2a["next_cursor"] is None


def test_snapshot_export_ignores_pending_events(client):
    _post(client, "x-gap", "x-gap-e0", 0, 100)
    _post(client, "x-gap", "x-gap-e2", 2, 300)  # buffered behind the gap at 1

    body = _page(client, "x-gap", limit=10).json()
    assert body["snapshot_next_sequence"] == 1
    assert body["balance"] == 100
    assert [e["sequence"] for e in body["events"]] == [0]
    assert body["next_cursor"] is None

    # Close the gap after the export: a new snapshot observes the catch-up.
    _post(client, "x-gap", "x-gap-e1", 1, 200)
    fresh = _page(client, "x-gap", limit=10).json()
    assert fresh["snapshot_next_sequence"] == 3
    assert fresh["balance"] == 600
    assert [e["sequence"] for e in fresh["events"]] == [0, 1, 2]


def test_snapshot_export_empty_stream(client):
    # A stream created by an empty fork has no events at all.
    _seed_deltas(client, "x-src", (1,))
    r = client.post(
        "/streams/x-src/forks",
        json={"fork_id": "f-empty", "target_stream_id": "x-empty", "through_sequence": 0},
    )
    assert r.status_code == 201

    body = _page(client, "x-empty", limit=10).json()
    assert body["snapshot_next_sequence"] == 0
    assert body["balance"] == 0
    assert body["events"] == []
    assert body["next_cursor"] is None


def test_snapshot_export_default_limit_is_100(client):
    _seed_applied_events(client, "x-default", 105)

    body = _page(client, "x-default").json()
    assert len(body["events"]) == 100
    assert body["snapshot_next_sequence"] == 105
    assert body["next_cursor"] is not None

    rest = _page(client, "x-default", cursor=body["next_cursor"]).json()
    assert [e["sequence"] for e in rest["events"]] == [100, 101, 102, 103, 104]
    assert rest["next_cursor"] is None


def test_snapshot_export_unknown_stream_returns_404(client):
    r = _page(client, "x-ghost")
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"

    # A valid cursor does not resurrect a missing stream either.
    _seed_deltas(client, "x-alive", (1, 2, 3))
    cursor = _page(client, "x-alive", limit=1).json()["next_cursor"]
    r = _page(client, "x-ghost", cursor=cursor)
    assert r.status_code == 404
    assert r.json()["detail"]["code"] == "stream_not_found"


def test_snapshot_export_limit_validation(client):
    _seed_deltas(client, "x-limit", (1,))
    assert _page(client, "x-limit", limit=0).status_code == 422
    assert _page(client, "x-limit", limit=-3).status_code == 422
    assert _page(client, "x-limit", limit=1001).status_code == 422
    assert client.get("/streams/x-limit/events?limit=abc").status_code == 422


def test_snapshot_cursor_is_signed_canonical_json(client):
    import base64
    import hashlib
    import hmac
    import json

    from app import config

    _seed_deltas(client, "x-cursor", (1, 2, 3))
    cursor = _page(client, "x-cursor", limit=1).json()["next_cursor"]

    body_b64, sig_b64 = cursor.split(".")
    payload = base64.urlsafe_b64decode(body_b64)
    # Canonical JSON: sorted keys, compact separators, exactly the three fields.
    assert payload == (
        b'{"next_sequence":1,"snapshot_next_sequence":3,"stream_id":"x-cursor"}'
    )
    expected_sig = hmac.new(
        config.snapshot_cursor_secret().encode("utf-8"), payload, hashlib.sha256
    ).digest()
    assert base64.urlsafe_b64decode(sig_b64) == expected_sig
    assert json.loads(payload) == {
        "stream_id": "x-cursor",
        "snapshot_next_sequence": 3,
        "next_sequence": 1,
    }


def test_snapshot_cursor_tampering_returns_400(client):
    _seed_deltas(client, "x-tamper", (1, 2, 3))
    cursor = _page(client, "x-tamper", limit=1).json()["next_cursor"]
    body_b64, sig_b64 = cursor.split(".")

    # Flip one payload character: the signature no longer matches.
    forged = ("A" if body_b64[0] != "A" else "B") + body_b64[1:] + "." + sig_b64
    r = _page(client, "x-tamper", cursor=forged)
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "invalid_cursor"

    # Flip one signature character.
    forged_sig = body_b64 + "." + ("A" if sig_b64[0] != "A" else "B") + sig_b64[1:]
    assert _page(client, "x-tamper", cursor=forged_sig).status_code == 400

    # Structurally invalid cursors.
    assert _page(client, "x-tamper", cursor="not-a-cursor").status_code == 400
    assert _page(client, "x-tamper", cursor="!!!.@@@").status_code == 400
    assert _page(client, "x-tamper", cursor="").status_code == 400
    assert _page(client, "x-tamper", cursor=cursor + ".extra").status_code == 400


def test_snapshot_cursor_cross_stream_returns_400(client):
    _seed_deltas(client, "x-one", (1, 2, 3))
    _seed_deltas(client, "x-two", (4, 5, 6))
    cursor = _page(client, "x-one", limit=1).json()["next_cursor"]

    r = _page(client, "x-two", cursor=cursor)
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "cursor_stream_mismatch"


def test_snapshot_cursor_invalid_fields_return_400(client):
    _seed_deltas(client, "x-fields", (1, 2, 3))

    bad_payloads = [
        {"stream_id": "x-fields"},  # missing fields
        {  # extra field
            "stream_id": "x-fields",
            "snapshot_next_sequence": 3,
            "next_sequence": 1,
            "balance": 6,
        },
        {"stream_id": "x-fields", "snapshot_next_sequence": 3, "next_sequence": 4},  # past bound
        {"stream_id": "x-fields", "snapshot_next_sequence": -1, "next_sequence": 0},
        {"stream_id": "x-fields", "snapshot_next_sequence": 3, "next_sequence": -1},
        {"stream_id": "x-fields", "snapshot_next_sequence": 3, "next_sequence": True},
        {"stream_id": "x-fields", "snapshot_next_sequence": "3", "next_sequence": 1},
        {"stream_id": "", "snapshot_next_sequence": 3, "next_sequence": 1},
        {"stream_id": 7, "snapshot_next_sequence": 3, "next_sequence": 1},
    ]
    for payload in bad_payloads:
        r = _page(client, "x-fields", cursor=_craft_cursor(payload))
        assert r.status_code == 400, payload
        assert r.json()["detail"]["code"] == "invalid_cursor", payload

    # Validly signed but not a JSON object at all.
    assert _page(client, "x-fields", cursor=_sign_payload(b"[1,2,3]")).status_code == 400
    assert _page(client, "x-fields", cursor=_sign_payload(b"not json")).status_code == 400


def test_snapshot_cursor_secret_change_invalidates_old_cursors(client, monkeypatch):
    _seed_deltas(client, "x-secret", (1, 2, 3))
    cursor = _page(client, "x-secret", limit=1).json()["next_cursor"]

    monkeypatch.setenv("SNAPSHOT_CURSOR_SECRET", "rotated-secret")
    r = _page(client, "x-secret", cursor=cursor)
    assert r.status_code == 400
    assert r.json()["detail"]["code"] == "invalid_cursor"

    # New cursors signed with the rotated secret work again.
    assert _page(client, "x-secret", limit=1).status_code == 200


def test_snapshot_export_unicode_stream_id_round_trip(client):
    _seed_deltas(client, "流-快照", (7, 8, 9))

    first = _page(client, "流-快照", limit=2).json()
    assert first["snapshot_next_sequence"] == 3
    assert [e["sequence"] for e in first["events"]] == [0, 1]

    last = _page(client, "流-快照", limit=2, cursor=first["next_cursor"]).json()
    assert [e["sequence"] for e in last["events"]] == [2]
    assert last["balance"] == 24
    assert last["next_cursor"] is None
