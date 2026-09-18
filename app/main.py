"""FastAPI application: out-of-order event submission and stream queries."""

from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from . import consumers, db, forks, repo, reversals, seals, snapshots, transfers


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.initialise()
    yield


app = FastAPI(
    title="Out-of-Order Event Merge API",
    version="1.0.0",
    description=(
        "Submit delta events with per-stream sequence numbers. Contiguous events "
        "are folded into the stream balance; gaps are buffered and caught up "
        "atomically when the missing event arrives."
    ),
    lifespan=lifespan,
)


class EventIn(BaseModel):
    event_id: str = Field(..., min_length=1, description="Client-assigned unique event id")
    sequence: int = Field(..., ge=0, description="Monotonic sequence within the stream, starting at 0")
    delta: int = Field(..., description="Integer amount added to the balance when the event is applied")

    @field_validator("sequence", "delta", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {"json_schema_extra": {"examples": [{"event_id": "evt-1", "sequence": 0, "delta": 100}]}}


class StreamStateOut(BaseModel):
    stream_id: str
    balance: int
    next_sequence: int
    pending_sequences: list[int]


class EventAck(BaseModel):
    status: str
    replayed: bool
    stream: StreamStateOut


class ClaimIn(BaseModel):
    max_events: int = Field(
        ..., ge=1, le=10_000, description="Maximum number of events in this batch"
    )

    @field_validator("max_events", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {"json_schema_extra": {"examples": [{"max_events": 100}]}}


class ClaimedEventOut(BaseModel):
    event_id: str
    sequence: int
    delta: int


class ClaimOut(BaseModel):
    stream_id: str
    consumer_id: str
    lease_token: str
    expires_at: int = Field(..., description="POSIX milliseconds; the lease is valid until then")
    from_sequence: int
    to_sequence: int = Field(..., description="Exclusive upper bound of the claimed batch")
    events: list[ClaimedEventOut]


class AckIn(BaseModel):
    lease_token: str = Field(..., min_length=1)

    model_config = {"json_schema_extra": {"examples": [{"lease_token": "<token from claim>"}]}}


class AckOut(BaseModel):
    status: str
    stream_id: str
    consumer_id: str
    cursor: int = Field(..., description="New cursor sequence (next event this consumer will receive)")


class NackIn(BaseModel):
    lease_token: str = Field(..., min_length=1)
    reason: str = Field(
        ..., min_length=1, description="Non-empty human-readable rejection reason"
    )

    @field_validator("lease_token", "reason")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [{"lease_token": "<token from claim>", "reason": "payment failed"}]
        }
    }


class NackOut(BaseModel):
    status: str
    stream_id: str
    consumer_id: str
    lease_token: str = Field(..., description="The lease token this nack settled (or replays)")
    cursor: int = Field(
        ...,
        description="Cursor after the nack: unchanged while the event is still retried, "
        "advanced one step past the event when it was dead-lettered",
    )
    attempts: int = Field(
        ..., description="Consecutive nack failures recorded for the batch's first event"
    )
    max_attempts: int = Field(..., description="Configured CONSUMER_MAX_ATTEMPTS threshold")
    dead_lettered: bool = Field(
        ..., description="True when this nack reached the threshold and dead-lettered the event"
    )
    reason: str = Field(..., description="The non-empty rejection reason")
    replayed: bool = Field(
        ..., description="True for an idempotent replay of an already-settled nack token"
    )


class DeadLetterOut(BaseModel):
    stream_id: str
    consumer_id: str
    sequence: int
    event_id: str
    delta: int
    attempts: int = Field(..., description="Consecutive nack failures that isolated the event")
    last_reason: str
    dead_lettered_at: int = Field(..., description="POSIX milliseconds isolation time")


class DeadLetterListOut(BaseModel):
    stream_id: str
    consumer_id: str
    dead_letters: list[DeadLetterOut]


class ForkIn(BaseModel):
    fork_id: str = Field(..., min_length=1, description="Client-assigned idempotency key, unique per source stream")
    target_stream_id: str = Field(..., min_length=1, description="Stream created by the fork; must not already exist")
    through_sequence: int = Field(..., ge=0, description="Exclusive upper bound; must be <= the source next_sequence")

    @field_validator("through_sequence", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [
                {"fork_id": "fork-1", "target_stream_id": "acct-1-clone", "through_sequence": 2}
            ]
        }
    }


class ForkOut(BaseModel):
    source_stream_id: str
    target_stream_id: str
    through_sequence: int = Field(..., description="Exclusive upper bound of the copied prefix")
    balance: int = Field(..., description="Initial balance of the target stream")
    next_sequence: int = Field(..., description="Initial next_sequence of the target stream")


class TransferIn(BaseModel):
    transfer_id: str = Field(..., min_length=1, description="Client-assigned global idempotency key")
    source_stream_id: str = Field(..., min_length=1, description="Stream debited by the transfer")
    target_stream_id: str = Field(..., min_length=1, description="Stream credited by the transfer")
    amount: int = Field(..., gt=0, description="Positive integer amount moved from source to target")

    @field_validator("amount", mode="before")
    @classmethod
    def _reject_non_int(cls, value):
        # In lax mode pydantic coerces bools/numeric strings to int; the amount
        # must be a JSON integer.
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError("must be an integer, not a boolean or string")
        return value

    @field_validator("source_stream_id", "target_stream_id")
    @classmethod
    def _reject_empty(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    @field_validator("target_stream_id")
    @classmethod
    def _streams_must_differ(cls, value: str, info) -> str:
        source = info.data.get("source_stream_id")
        if source is not None and source == value:
            raise ValueError("source_stream_id and target_stream_id must be different")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [
                {
                    "transfer_id": "txn-1",
                    "source_stream_id": "acct-1",
                    "target_stream_id": "acct-2",
                    "amount": 50,
                }
            ]
        }
    }


class TransferSideOut(BaseModel):
    stream_id: str
    event_id: str
    sequence: int = Field(..., description="Sequence the debit/credit event was appended at")
    state: StreamStateOut = Field(..., description="Latest state of the stream after applying the event")


class TransferOut(BaseModel):
    status: str
    replayed: bool
    transfer_id: str
    amount: int
    source: TransferSideOut
    target: TransferSideOut


class ReversalIn(BaseModel):
    reversal_id: str = Field(..., min_length=1, description="Client-assigned global idempotency key")

    @field_validator("reversal_id")
    @classmethod
    def _reject_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be empty")
        return value

    model_config = {"json_schema_extra": {"examples": [{"reversal_id": "rev-1"}]}}


class ReversalOut(BaseModel):
    status: str
    replayed: bool
    reversal_id: str
    stream_id: str
    event_id: str = Field(..., description="The original event that was reversed")
    reversal_event_id: str = Field(..., description="Event id of the appended compensation event")
    sequence: int = Field(..., description="Sequence the compensation event was appended at")
    delta: int = Field(..., description="Compensation delta (negated original delta)")
    stream: StreamStateOut = Field(..., description="Latest state of the stream after the reversal")


class SealIn(BaseModel):
    seal_id: str = Field(
        ..., min_length=1, description="Client-assigned idempotency key, unique per stream"
    )
    expected_next_sequence: int = Field(
        ..., ge=0, description="The stream version that must be current for the seal to apply"
    )

    @field_validator("expected_next_sequence", mode="before")
    @classmethod
    def _reject_bool(cls, value):
        # In lax mode pydantic accepts bool as int; JSON booleans are not integers here.
        if isinstance(value, bool):
            raise ValueError("must be an integer, not a boolean")
        return value

    model_config = {
        "json_schema_extra": {
            "examples": [{"seal_id": "seal-1", "expected_next_sequence": 3}]
        }
    }


class SealOut(BaseModel):
    status: str
    replayed: bool
    stream_id: str
    seal_id: str
    next_sequence: int = Field(..., description="Sealed version (the applied-prefix length)")
    event_count: int = Field(..., description="Number of events covered by the hash")
    sha256: str = Field(..., description="SHA-256 of the canonical newline-joined JSON events")


class SnapshotEventOut(BaseModel):
    event_id: str
    sequence: int
    delta: int


class EventPageOut(BaseModel):
    stream_id: str
    snapshot_next_sequence: int = Field(
        ..., description="Snapshot upper bound (exclusive) pinned by the first request"
    )
    balance: int = Field(
        ..., description="Balance at the snapshot version (applied deltas below the bound)"
    )
    events: list[SnapshotEventOut]
    next_cursor: str | None = Field(
        ..., description="Opaque cursor for the next page; null on the last page"
    )


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post(
    "/streams/{stream_id}/events",
    response_model=EventAck,
    status_code=201,
    responses={200: {"model": EventAck, "description": "Idempotent replay of an existing event"}},
    tags=["events"],
    summary="Submit one event for a stream",
)
def submit_event(stream_id: str, event: EventIn):
    try:
        state, replayed = repo.submit_event(
            stream_id, event.event_id, event.sequence, event.delta
        )
    except repo.ConflictError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )

    payload = EventAck(
        status="ok", replayed=replayed, stream=StreamStateOut(**state.as_dict())
    ).model_dump()
    # Idempotent replays return 200; first-time inserts return 201.
    return JSONResponse(payload, status_code=200 if replayed else 201)


@app.get(
    "/streams/{stream_id}",
    response_model=StreamStateOut,
    tags=["streams"],
    summary="Get the current balance, next sequence and pending sequences",
)
def get_stream(stream_id: str) -> StreamStateOut:
    state = repo.get_stream_state(stream_id)
    if state is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return StreamStateOut(**state.as_dict())


@app.get(
    "/streams/{stream_id}/events",
    response_model=EventPageOut,
    tags=["events"],
    summary="Export the applied event history as a stable, paginated snapshot",
)
def list_stream_events(
    stream_id: str,
    limit: int = Query(
        100, ge=1, le=1000, description="Maximum number of events per page"
    ),
    cursor: str | None = Query(
        None,
        description="Opaque continuation cursor (the next_cursor of the previous page); "
        "omit it to pin a fresh snapshot",
    ),
) -> EventPageOut:
    try:
        page = snapshots.get_events_page(stream_id, limit, cursor)
    except snapshots.CursorError as exc:
        raise HTTPException(
            status_code=400,
            detail={"code": exc.code, "message": exc.message},
        )
    if page is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return EventPageOut(**page.as_dict())


@app.post(
    "/streams/{stream_id}/forks",
    response_model=ForkOut,
    status_code=201,
    responses={200: {"model": ForkOut, "description": "Idempotent replay of an existing fork"}},
    tags=["forks"],
    summary="Fork the applied [0, through_sequence) prefix into a new stream",
)
def fork_stream(stream_id: str, body: ForkIn):
    try:
        fork = forks.create_fork(
            stream_id, body.fork_id, body.target_stream_id, body.through_sequence
        )
    except forks.ForkError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints (fork id, target stream)
        # map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )
    if fork is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )

    payload = ForkOut(
        source_stream_id=fork.source_stream_id,
        target_stream_id=fork.target_stream_id,
        through_sequence=fork.through_sequence,
        balance=fork.balance,
        next_sequence=fork.next_sequence,
    ).model_dump()
    # Idempotent replays return 200; first-time forks return 201.
    return JSONResponse(payload, status_code=200 if fork.replayed else 201)


@app.post(
    "/streams/{stream_id}/seal",
    response_model=SealOut,
    status_code=201,
    responses={200: {"model": SealOut, "description": "Idempotent replay of an existing seal"}},
    tags=["seals"],
    summary="Irreversibly seal a stream and hash its applied event prefix",
)
def seal_stream(stream_id: str, body: SealIn):
    try:
        result = seals.create_seal(stream_id, body.seal_id, body.expected_next_sequence)
    except seals.SealError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the seals primary key / events seal trigger map
        # to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )

    payload = SealOut(
        status="ok",
        replayed=result.replayed,
        stream_id=result.stream_id,
        seal_id=result.seal_id,
        next_sequence=result.next_sequence,
        event_count=result.event_count,
        sha256=result.sha256,
    ).model_dump()
    # Idempotent replays return 200; first-time seals return 201.
    return JSONResponse(payload, status_code=200 if result.replayed else 201)


def _transfer_payload(result: transfers.TransferResult) -> dict:
    return TransferOut(
        status="ok",
        replayed=result.replayed,
        transfer_id=result.transfer_id,
        amount=result.amount,
        source=TransferSideOut(
            stream_id=result.source_stream_id,
            event_id=result.source_event_id,
            sequence=result.source_sequence,
            state=StreamStateOut(**result.source.as_dict()),
        ),
        target=TransferSideOut(
            stream_id=result.target_stream_id,
            event_id=result.target_event_id,
            sequence=result.target_sequence,
            state=StreamStateOut(**result.target.as_dict()),
        ),
    ).model_dump()


@app.post(
    "/transfers",
    response_model=TransferOut,
    status_code=201,
    responses={200: {"model": TransferOut, "description": "Idempotent replay of an existing transfer"}},
    tags=["transfers"],
    summary="Atomically debit one stream and credit another stream",
)
def transfer(body: TransferIn):
    try:
        result = transfers.create_transfer(
            body.transfer_id,
            body.source_stream_id,
            body.target_stream_id,
            body.amount,
        )
    except transfers.TransferError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )
    # Idempotent replays return 200; first-time transfers return 201.
    return JSONResponse(_transfer_payload(result), status_code=200 if result.replayed else 201)


def _reversal_payload(result: reversals.ReversalResult) -> dict:
    return ReversalOut(
        status="ok",
        replayed=result.replayed,
        reversal_id=result.reversal_id,
        stream_id=result.stream_id,
        event_id=result.event_id,
        reversal_event_id=result.reversal_event_id,
        sequence=result.sequence,
        delta=result.delta,
        stream=StreamStateOut(**result.state.as_dict()),
    ).model_dump()


@app.post(
    "/streams/{stream_id}/events/{event_id}/reversals",
    response_model=ReversalOut,
    status_code=201,
    responses={200: {"model": ReversalOut, "description": "Idempotent replay of an existing reversal"}},
    tags=["reversals"],
    summary="Reverse an applied event by appending a compensation event",
)
def reverse_event(stream_id: str, event_id: str, body: ReversalIn):
    try:
        result = reversals.create_reversal(stream_id, event_id, body.reversal_id)
    except reversals.ReversalError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail={"code": exc.code, "message": exc.message},
        )
    except sqlite3.IntegrityError as exc:
        # Defence in depth: the UNIQUE constraints map to a 409 as well.
        raise HTTPException(
            status_code=409,
            detail={"code": "integrity_conflict", "message": str(exc)},
        )
    # Idempotent replays return 200; first-time reversals return 201.
    return JSONResponse(_reversal_payload(result), status_code=200 if result.replayed else 201)


@app.post(
    "/streams/{stream_id}/consumers/{consumer_id}/claim",
    response_model=ClaimOut,
    status_code=200,
    tags=["consumers"],
    summary="Claim the next contiguous applied event batch (with a lease token)",
)
def claim_batch(stream_id: str, consumer_id: str, body: ClaimIn) -> ClaimOut:
    claim = consumers.claim(stream_id, consumer_id, body.max_events)
    if claim is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return ClaimOut(**claim.as_dict())


@app.post(
    "/streams/{stream_id}/consumers/{consumer_id}/ack",
    response_model=AckOut,
    status_code=200,
    tags=["consumers"],
    summary="Acknowledge a claimed batch and atomically advance the cursor",
)
def ack_batch(stream_id: str, consumer_id: str, body: AckIn) -> AckOut:
    try:
        cursor = consumers.ack(stream_id, consumer_id, body.lease_token)
    except consumers.LeaseError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    if cursor is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return AckOut(
        status="ok",
        stream_id=stream_id,
        consumer_id=consumer_id,
        cursor=cursor,
    )


@app.post(
    "/streams/{stream_id}/consumers/{consumer_id}/nack",
    response_model=NackOut,
    status_code=200,
    tags=["consumers"],
    summary="Reject a claimed batch: retry its first event or dead-letter it at the threshold",
)
def nack_batch(stream_id: str, consumer_id: str, body: NackIn) -> NackOut:
    try:
        result = consumers.nack(
            stream_id, consumer_id, body.lease_token, body.reason
        )
    except consumers.LeaseError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": exc.code, "message": exc.message},
        )
    if result is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return NackOut(**result.as_dict())


@app.get(
    "/streams/{stream_id}/consumers/{consumer_id}/dead-letters",
    response_model=DeadLetterListOut,
    tags=["consumers"],
    summary="List a consumer's dead-letter records ordered by sequence ascending",
)
def list_dead_letters(stream_id: str, consumer_id: str) -> DeadLetterListOut:
    records = consumers.list_dead_letters(stream_id, consumer_id)
    if records is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "stream_not_found", "message": f"unknown stream {stream_id!r}"},
        )
    return DeadLetterListOut(
        stream_id=stream_id,
        consumer_id=consumer_id,
        dead_letters=[DeadLetterOut(**r.as_dict()) for r in records],
    )
