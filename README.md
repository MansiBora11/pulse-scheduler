# Pulse

A durable job scheduler with heartbeat-based leases, retries with backoff, a dead-letter queue and crash recovery.

Producers submit jobs over HTTP. Workers claim a job with a **lease** and keep it alive with **heartbeats**. If a worker dies, the heartbeats stop, the lease expires, and the job goes to another worker. State lives in DynamoDB, and every state change is a conditional write, so two workers can never hold the same lease.

> **Status:** early development. Single scheduler process, DynamoDB storage, at-least-once delivery. See [docs/DESIGN.md](docs/DESIGN.md) for the exact guarantees and limits.

## At a glance

- **Leases and heartbeats:** atomic job claiming via DynamoDB conditional writes; expired leases are reclaimed by a reaper.
- **Retries:** exponential backoff with full jitter, then a dead-letter queue after `maxAttempts`.
- **Idempotent submit:** a client-chosen job ID makes retries safe.
- **Delayed jobs:** `runAt` schedules a job for later.
- **Polyglot workers:** Java SDK (`pulse-client`) and Go worker (`go/`), same HTTP API.
- **Proof, not claims:** a load generator, crash-injecting workers and a checker that verifies no job was lost and none ran early.

## Layout

| Path | What it is |
|---|---|
| `pulse-server/` | Java 21 scheduler: HTTP API, DynamoDB store, lease reaper |
| `pulse-client/` | Java client and worker SDK, plus a sample worker |
| `go/pulse/` | Go client library |
| `go/cmd/loadgen` | Submits jobs and records every accepted one |
| `go/cmd/worker` | Go worker with fast, slow, flaky, poison and crasher handlers |
| `go/cmd/verify` | Checks a finished run: nothing lost, nothing early |
| `docs/DESIGN.md` | State machine, table schema, guarantees, limits |

## Quick start

Requires JDK 21, Maven, Go 1.24 and Docker.

```bash
make up          # DynamoDB Local + Pulse server on :8080
make go-build    # builds bin/loadgen, bin/worker, bin/verify
```

Submit a job and watch it move:

```bash
curl -s -XPOST localhost:8080/v1/jobs -d '{"id":"hello-1","type":"fast"}'
bin/worker &                       # claims and runs it
curl -s localhost:8080/v1/jobs/hello-1
```

## Crash-recovery demo

Run workers that kill themselves after applying a job's effect but before acknowledging it, then prove nothing was lost:

```bash
# a worker that keeps getting killed and restarted
( while true; do bin/worker -crash-pct 30 -lease 5s; sleep 1; done ) &

bin/loadgen -n 5000 -mix fast:60,slow:20,flaky:10,poison:5,crasher:5
sleep 120                          # let retries and lease expiries drain
bin/verify -url http://localhost:8080
```

`verify` exits non-zero if any submitted job was neither executed nor dead-lettered, ran before its `runAt`, or ran without being submitted. Duplicate executions are reported but allowed, because delivery is at-least-once.

## API

| Method and path | Purpose |
|---|---|
| `POST /v1/jobs` | Submit. Body: `type`, optional `id`, `payload`, `runAt` (epoch ms), `maxAttempts`, `priority` |
| `POST /v1/jobs/poll` | Claim one due job. Body: `workerId`, `types`, `leaseMs`. `204` if none |
| `POST /v1/jobs/{id}/heartbeat` | Extend the lease |
| `POST /v1/jobs/{id}/ack` | Mark done |
| `POST /v1/jobs/{id}/fail` | Report failure: retry with backoff, or dead-letter |
| `GET /v1/jobs/{id}` | Inspect a job |
| `GET /v1/dlq` | List dead jobs |

Lease operations return `409 lease_lost` when the caller no longer holds the lease.

## Results


| Scenario | Jobs | Workers | Throughput | p99 submit latency | Missing | Duplicates |
|---|---|---|---|---|---|---|
| Steady, fast handler | | | | | | |
| 20% failures with retries | | | | | | |
| Workers killed mid-job | | | | | | |

## Roadmap

- [x] Submit, poll, heartbeat, ack, fail over HTTP
- [x] Leases, reaper, backoff, dead-letter queue
- [x] Go load generator, worker and run verifier
- [ ] Integration tests against DynamoDB Local
- [ ] Priorities and fair ordering
- [ ] Shard the status index to remove the hot partition
- [ ] Cron schedules
- [ ] Metrics (queue depth, lease expiries, schedule delay) and Grafana dashboard
- [ ] Randomized kill-and-verify chaos script
- [ ] Terraform deployment to AWS (ECS Fargate, DynamoDB, CloudWatch)
