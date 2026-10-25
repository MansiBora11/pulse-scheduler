# Pulse design

## Goal

Run each submitted job on some worker even when workers or the scheduler crash, without a job being claimed by two workers at once.

## Components

```
producer ──POST /v1/jobs──▶ ┌────────────────┐        ┌──────────┐
                            │  pulse-server  │◀──────▶│ DynamoDB │
worker ──poll/heartbeat/──▶ │  HTTP + reaper │        └──────────┘
        ack/fail            └────────────────┘
```

The server is stateless. All state is in one DynamoDB table, so several server instances can run side by side.

## Job state machine

```
            lease                 ack
  READY ─────────────▶ LEASED ─────────────▶ DONE
    ▲                    │
    │  fail / lease      │ fail / lease expired,
    │  expired,          │ attempts >= maxAttempts
    │  attempts left     ▼
    └───────────────── (backoff)             DEAD
```

- `READY` becomes `LEASED` when a worker claims it. `attempts` is incremented at claim time.
- `LEASED` becomes `DONE` on ack, `READY` on failure or lease expiry (after a backoff delay), or `DEAD` when `attempts >= maxAttempts`.
- `DEAD` jobs are the dead-letter queue, listed by `GET /v1/dlq`.

## Table schema

Table `pulse_jobs`, partition key `jobId`.

| Attribute | Meaning |
|---|---|
| `status` | READY, LEASED, DONE or DEAD |
| `dueAt` | When something must happen next: `runAt` while READY, `leaseUntil` while LEASED, the time of death for DEAD. Removed on DONE |
| `leaseToken` | Random token issued at claim time, required for heartbeat, ack and fail |
| `attempts`, `maxAttempts` | Attempts used and allowed |
| `runAt`, `priority`, `type`, `payload`, `lastError`, `createdAt` | Job data |

Global secondary index `status-dueAt-index`: partition key `status`, sort key `dueAt`, all attributes projected. One index answers all three scheduler questions:

- Which READY jobs are due? Query `status = READY AND dueAt <= now`.
- Which leases expired? Query `status = LEASED AND dueAt <= now`.
- What is in the DLQ? Query `status = DEAD`.

Because DONE jobs have no `dueAt`, they leave the index (a sparse index), so the index only holds work that is still live.

## Lease protocol

1. **Claim.** Query the index for due READY jobs, then try a conditional update on the base table: `status = READY AND dueAt <= now`, set `LEASED`, a fresh `leaseToken` and `leaseUntil = now + leaseMs`. If another worker won, the condition fails and the next candidate is tried.
2. **Heartbeat.** The worker extends `leaseUntil` roughly every `leaseMs / 3`. The update is conditional on the exact `leaseToken`. A rejected heartbeat (`409 lease_lost`) tells the worker to stop.
3. **Ack / fail.** Both are conditional on the exact `leaseToken`.
4. **Reap.** A background loop finds LEASED jobs whose `dueAt` passed and fails them with "lease expired". The update is conditional on the token and on `leaseUntil <= now`, so a lease renewed a moment ago is never reclaimed.

The index is eventually consistent and is only used to find candidates. Every state change is a conditional write on the base table, so a stale index read can waste a request but cannot break exclusivity.

## Guarantees

- **Exclusive lease.** At any moment at most one lease token is valid for a job. This follows from the conditional writes above.
- **At-least-once execution.** A job whose worker crashes is retried after its lease expires, until `maxAttempts` is reached.
- **No early execution.** A job is not claimed before `runAt`, judged by the scheduler's clock.
- **Idempotent submit.** Resubmitting an existing job ID returns the stored job and creates nothing.
- **Bounded retries.** After `maxAttempts` the job is dead-lettered with its last error.

## Non-guarantees and known limits

- **Duplicates happen.** If a worker applies a job's effect and dies before acking, or its lease expires while it is still running, the job runs again. Handlers must be idempotent. A late ack from a worker that lost its lease is rejected, even if the work finished.
- **Not exactly-once.** Exactly-once effects need an idempotency key in the downstream system. Pulse gives you the job ID and attempt number for that.
- **Clock dependence.** `runAt`, leases and backoff use the server's wall clock. Instances with skewed clocks shift lease expiry and scheduling by the skew. Keep servers on NTP.
- **Hot index partition.** Every READY job shares the partition key `READY`, which caps throughput well below what DynamoDB offers across many keys. The fix is to shard the key (`READY#0` to `READY#N`) and have workers query shards in random order. This is the first scaling item on the roadmap.
- **Approximate ordering.** `lease` shuffles the 25 earliest candidates to reduce contention between concurrent pollers, so jobs run roughly in `dueAt` order, not strictly. `priority` is stored but not yet honoured.
- **Polling.** Workers poll. There is no long-poll or push yet, so an idle queue costs requests and adds up to the poll interval of latency.
- **Type filtering after the query.** The index is not partitioned by job type, so a worker that handles only some types may read candidates it cannot take.
- **No authentication or multi-tenancy.** Run it on a private network.
- **Reaper on every instance.** Each server instance runs a reaper. This is safe because reclaiming is conditional, but it multiplies idle index reads.

## Why DynamoDB conditional writes

The claim step needs "take this job only if nobody holds it" as a single atomic operation. DynamoDB provides it per item with a condition expression, with no locks and no transactions. The tradeoff is that this project builds a scheduler on top of a database rather than a storage engine, so throughput is bounded by DynamoDB and its cost. Report that honestly in benchmarks, and say what you would change (batched claims, sharded status keys, a dedicated log) to go further.

## Testing strategy

- **Unit:** backoff bounds (done); state-transition logic against a fake store.
- **Integration:** the full lifecycle against DynamoDB Local, including two workers racing for one job and a heartbeat racing the reaper.
- **Run-level correctness:** `go/cmd/loadgen` records accepted jobs, workers record applied effects, and `go/cmd/verify` checks that no job is missing or early. Chaos runs repeat this with workers killed at random moments (`kill -9`) and the scheduler restarted mid-run.
