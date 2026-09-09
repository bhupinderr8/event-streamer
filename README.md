# Event Streamer

A high-throughput event ingestion pipeline built in Go. Accepts events over gRPC (unary, batch, and streaming), rate-limits per tenant via Redis, buffers through a sharded worker pool, publishes to Apache Kafka, and persists to PostgreSQL with at-least-once delivery guarantees.

## Architecture

```text
                               ┌──────────────────────────────────────────┐
                               │       Ingestion Server (:50051)          │
Clients ──── gRPC/HTTP2 ────▶  │                                          │
                               │  Rate Limiter ──▶ Worker Pool ──▶ Kafka  │
                               │   (Redis)         (32 shards)            │
                               └──────────────────────────────────────────┘
                                                                     │
                                                                     ▼
                                                              Consumer Daemon
                                                              (Kafka → PostgreSQL)
```

**Key design decisions:**

- **2-tier rate limiting** — Local atomic counters serve the fast path. Redis lease renewal is coordinated via singleflight to avoid stampedes.
- **Sharded MPSC worker pool** — 32 independent channel shards eliminate Go channel lock contention under high concurrency.
- **Async Kafka writes with DLQ** — Failed async publishes are captured to a local dead-letter queue file for crash recovery.
- **At-least-once persistence** — Kafka consumer offsets are committed only after successful PostgreSQL batch inserts.

## Quick Start

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) with Compose V2
- [Go 1.22+](https://go.dev/dl/) (for local development)

### Run with Docker Compose

```bash
# Start all services (Postgres, Redis, Kafka, Server, Consumer)
docker compose up -d --build

# Verify health
curl -s http://127.0.0.1:8080/healthz
```

### Run Benchmarks

```bash
# Unary mode
docker compose run --rm bench -addr=server:50051 -mode=unary -duration=10s -concurrency=192 -conns=24

# Batch mode (100 events/batch)
docker compose run --rm bench -addr=server:50051 -mode=batch -duration=10s -concurrency=64 -conns=8 -batch-size=100

# Streaming mode
docker compose run --rm bench -addr=server:50051 -mode=stream -duration=10s -concurrency=8 -conns=2
```

### Stop

```bash
docker compose down
```

## Local Development

Build and run Go binaries natively while infrastructure runs in Docker:

```bash
# Start only infrastructure
docker compose up -d postgres redis kafka

# Build
go build -o bin/server ./cmd/server
go build -o bin/consumer ./cmd/consumer
go build -o bin/bench ./cmd/bench

# Run server
REDIS_ADDR=127.0.0.1:6379 KAFKA_BROKERS=127.0.0.1:9092 ./bin/server

# Run consumer (requires DATABASE_URL)
DATABASE_URL="postgres://streamer:streamer_pass@127.0.0.1:5432/events_db?sslmode=disable" ./bin/consumer

# Run tests (requires Redis, Kafka, PostgreSQL)
go test -v -race ./...
```

## Configuration

All configuration is via environment variables. See [`.env.example`](.env.example) for a complete template.

### Server (`cmd/server`)

| Variable | Default | Description |
|---|---|---|
| `PORT` | `:50051` | gRPC listener address |
| `ADMIN_PORT` | `:8080` | HTTP admin server (`/healthz`, `/readyz`, `/metrics`, `/debug/pprof/`) |
| `REDIS_ADDR` | `127.0.0.1:6379` | Redis address for rate limiting |
| `KAFKA_BROKERS` | `127.0.0.1:9092` | Kafka broker address |
| `DLQ_PATH` | `dlq.log` | Dead-letter queue file path for failed Kafka writes |
| `RATE_LIMIT` | `100000` | Per-tenant request quota per second |
| `LEASE_BATCH_SIZE` | `2000` | Tokens leased per Redis round-trip |
| `LOG_LEVEL` | `info` | Log verbosity: `info`, `warn`, `error` |
| `GOMEMLIMIT` | — | Go runtime memory ceiling (e.g. `1500MiB`) |

### Consumer (`cmd/consumer`)

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | **(required)** | PostgreSQL connection URI |
| `KAFKA_BROKERS` | `127.0.0.1:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `events` | Kafka topic to consume |
| `KAFKA_GROUP_ID` | `event-streamer-consumers` | Consumer group ID |
| `BATCH_SIZE` | `1000` | Max events per database batch insert |
| `BATCH_TIMEOUT_MS` | `50` | Max wait before flushing a partial batch |

## Project Structure

```
├── cmd/
│   ├── server/       # gRPC ingestion server + HTTP admin
│   ├── consumer/     # Kafka → PostgreSQL consumer daemon
│   └── bench/        # Load testing tool (unary/batch/stream modes)
├── internal/
│   ├── broker/       # Kafka producer with batching, compression, DLQ
│   ├── limiter/      # Redis 2-tier token lease rate limiter
│   ├── workerpool/   # Sharded MPSC worker pool
│   └── storage/      # PostgreSQL persistence (pgx batch inserts)
├── proto/            # Protocol Buffer definitions and generated code
├── Dockerfile        # Multi-stage build (server, consumer, bench)
├── docker-compose.yml
└── .env.example
```

## API

The gRPC service is defined in [`proto/event.proto`](proto/event.proto) under `event.v1.EventService`:

| RPC | Description |
|---|---|
| `Ingest` | Single event ingestion (unary) |
| `IngestBatch` | Batch ingestion in one round-trip |
| `IngestStream` | Client-streaming for continuous pipelines |

## Observability

The admin HTTP server exposes:

- **`/healthz`** — Health check (JSON)
- **`/readyz`** — Readiness probe
- **`/metrics`** — Prometheus text format: `event_streamer_events_accepted_total`, `event_streamer_events_rejected_total`, `event_streamer_worker_queue_depth`, `event_streamer_kafka_async_errors_total`, `event_streamer_dlq_persisted_total`, and more
- **`/debug/pprof/`** — Go runtime profiling (CPU, heap, goroutines)

## Graceful Shutdown

On `SIGINT` / `SIGTERM`:

1. gRPC health status → `NOT_SERVING` (upstream load balancers stop routing)
2. HTTP admin server drained
3. In-flight RPCs and buffered worker queues flushed (10s timeout)
4. Kafka producer and Redis connections closed

## Security Notes

- The gRPC server does **not** include TLS or authentication. Use a service mesh, reverse proxy, or add interceptors for production.
- `/debug/pprof/` endpoints are bound to the admin port. Restrict access in production.
- `DATABASE_URL` is required as an environment variable — no credentials are hardcoded in source code.
- The DLQ file is created with `0600` permissions (owner-only read/write).
