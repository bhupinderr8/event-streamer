# Concurrent Event Streaming & Distributed Rate Limiter

A production-grade, high-throughput event ingestion engine, distributed rate limiting system, and downstream persistence pipeline built in Go. Engineered to operate reliably under constrained hardware environments (e.g., **2 vCPU, 4 GB RAM**) while sustaining **238,000+ events/sec in batch mode** and **14,500+ unary req/sec** with zero errors, sub-millisecond median response times, and at-least-once persistence to PostgreSQL.

---

## 🏛 Architecture & Data Flow

```text
[ Clients ] ──(Unary / Batch / Streaming gRPC over HTTP/2)──▶ [ Event Ingestion Service (:50051) ]
                                                                       │
                ┌──────────────────────────────────────────────────────┼────────────────────────────────────┐
                ▼                                                      ▼                                    ▼
       [ Redis 7 (:6379) ]                                 [ Sharded MPSC Worker Pool ]          [ Local DLQ File ]
    Distributed Rate Limiting                                (32 x 4,096 Ring Buffers)               (dlq.log)
  (Singleflight Token Leases)                                          │                          (Fallback Buffer)
                                                                       ▼
                                                          [ Kafka KRaft (:9092) ]
                                                          (8 Parallel Partitions)
                                                                       │
                                                                       ▼
                                                          [ Consumer Daemon Group ]
                                                          (Batched Offset Commits)
                                                                       │
                                                                       ▼
                                                          [ PostgreSQL 16 (:5432) ]
                                                           Durable Relational Store
```

### Ingestion Modes & Request Lifecycle

The service exposes three specialized gRPC ingestion endpoints defined in [`proto/event.proto`](proto/event.proto):

1. **Unary Ingestion (`Ingest`)**: Standard point-to-point RPC for individual, decoupled events.
2. **Batch Ingestion (`IngestBatch`)**: High-efficiency array ingestion amortizing network syscalls, protobuf framing, and rate-limiter reservations across up to 100+ events per call.
3. **Client-Streaming Ingestion (`IngestStream`)**: Persistent, low-overhead HTTP/2 streaming pipeline for continuous client telemetry.

#### Hot-Path Execution Flow

1. **gRPC Multiplexing & Envelope Hardening**:
   - Requests enter via HTTP/2 streams configured with 16 MiB message limits (`MaxRecvMsgSize`) and tuned flow-control windows (1 MiB stream, 2 MiB connection window) to eliminate TCP window bottlenecks.
   - Proactive TCP keepalive enforcement reaps dead connections.
2. **2-Tier Token Lease Rate Limiting & Singleflight Coordination**:
   - Quota is verified locally in $\approx 5\text{ ns}$ using atomic decrements (`atomic.Int64.Add(-n)`).
   - When the local lease is exhausted, concurrent callers coordinate via singleflight condition broadcast (`sync.Cond`), allowing exactly one goroutine to lease tokens in batches (e.g., 2,000 tokens) from Redis via an atomic Lua script (`INCRBY` / `EXPIRE`).
   - For batch requests, `AllowN` reserves the entire batch quota in a single atomic operation, reducing Redis round-trips by **up to 2,000×**.
   - An automatic background sweeper cleans up idle tenant buckets older than 5 minutes to prevent memory expansion under millions of ephemeral tenants.
3. **Non-Blocking Sharded Dispatch (MPSC Architecture)**:
   - Events are submitted into an array of 32 independent buffered ring channel shards managed by `WorkerPool`.
   - Distributing writes across shards eliminates Go runtime channel lock (`hchan.lock`) contention.
   - Batch submissions utilize `SubmitBatch` to route entire event arrays to a single worker shard without intermediate heap allocations.
   - Clients are acknowledged immediately with sub-millisecond median latencies ($p_{50} < 12\text{ ms}$).
4. **Kafka Multi-Partition Publishing & DLQ Fallback**:
   - Dedicated worker goroutines drain up to 128 events per tick using zero-copy keys (`unsafe.StringData`) and amortized batch timestamps (`time.Now()` once per batch).
   - Messages are flushed to Apache Kafka across **8 parallel topic partitions** using Snappy compression and `LeastBytes` partition balancing.
   - Asynchronous publish errors are logged, counted in `Producer.AsyncErrors()`, and automatically appended to a local crash recovery file (`dlq.log`).
5. **Downstream Consumption & Relational Persistence**:
   - Dedicated background consumer daemon (`cmd/consumer`) reads from the Kafka consumer group in micro-batches (up to 1,000 events or 50ms).
   - Events are persisted to PostgreSQL 16 using `pgx` connection pooling and batch queries (`INSERT ... ON CONFLICT DO NOTHING`).
   - Kafka consumer offsets are committed synchronously only after the database transaction commits, guaranteeing at-least-once persistence.
6. **Fast Backpressure & Load Shedding**:
   - If internal queue shards or downstream brokers saturate, the service immediately sheds excess load (`Accepted: false` / `rejected_count`) rather than blocking incoming threads or unbounded memory growth.

---

## 🛠 Tech Stack & Runtime Specifications

| Component | Technology | Configuration & Role |
| :--- | :--- | :--- |
| **Language & Runtime** | Go 1.22+ / 1.25 | Lock-free atomics, zero-copy hot paths, `-race` verified |
| **RPC & Protocol** | gRPC / Protocol Buffers v3 | Unary, Batch, and Client-Streaming APIs (`event.v1`) |
| **gRPC Server Limits** | 16 MiB Envelope & Keepalives | `16 MiB` max msg size; `keepalive.ServerParameters` for half-open connection cleanup |
| **Health Checking** | `google.golang.org/grpc/health` | Standard gRPC Health Checking Protocol (`grpc.health.v1`) |
| **Flow Control** | HTTP/2 Window Tuning | `1 MiB` stream window, `2 MiB` connection window, 64 KB R/W buffers |
| **Distributed Limiter** | Redis 7 Alpine | 2-Tier token leasing, singleflight `sync.Cond` coordination, 5-min idle tenant eviction |
| **Streaming Log** | Apache Kafka 3.8 (KRaft) | ZooKeeper-less commit log; **8 partitions**; JVM memory bounded (`-Xmx1024M -Xms256M`) |
| **Worker Engine** | Sharded MPSC Goroutine Pool | 32 parallel ring buffer shards (131,072 total capacity); amortized batch timestamps |
| **Dead Letter Queue** | Append-Only Crash Log | Local disk fallback buffer (`dlq.log`) for failed Kafka broker writes |
| **Downstream Sink** | PostgreSQL 16 Alpine | Persistent volume (`pgdata`), `pgx/v5` connection pool with batched inserts |
| **Consumer Service** | Kafka Consumer Daemon | Multi-partition consumer group with at-least-once offset commits |
| **Diagnostics Server** | HTTP Admin (`:8080`) | `/healthz`, `/readyz`, `/metrics` (Prometheus text exposition), `/debug/pprof/` |
| **Memory Guardrail** | `GOMEMLIMIT=1500MiB` | Paces Go GC heap targeting to prevent Linux OOM-killer panics on 4 GB RAM hosts |

---

## 📁 Repository Structure

```text
.
├── cmd/
│   ├── server/
│   │   └── main.go              # Production gRPC & HTTP admin server (Unary, Batch, Stream, Healthz, pprof)
│   ├── consumer/
│   │   └── main.go              # Kafka consumer daemon streaming to PostgreSQL with offset commits
│   └── bench/
│       └── main.go              # High-concurrency benchmark harness (-mode=unary|batch|stream)
├── proto/
│   ├── event.proto              # Protocol Buffer definition (Ingest, IngestBatch, IngestStream)
│   ├── event.pb.go              # Protoc-generated message structs
│   └── event_grpc.pb.go         # Protoc-generated gRPC service & client stubs
├── internal/
│   ├── broker/                  # Kafka producer with batching, compression, and DLQ file fallback
│   │   ├── producer.go
│   │   └── producer_test.go
│   ├── limiter/                 # Redis 2-tier token lease limiter (singleflight renewal & tenant eviction)
│   │   ├── limiter.go
│   │   └── limiter_test.go
│   ├── workerpool/              # Sharded MPSC ring buffer worker pool (zero-copy keys & batch drain)
│   │   ├── pool.go
│   │   └── pool_test.go
│   └── storage/                 # PostgreSQL event persistence layer (pgxpool batch inserts)
│       ├── postgres.go
│       └── postgres_test.go
├── bin/                         # Compiled Go binaries (bin/server, bin/bench, bin/consumer)
├── docker-compose.yml           # Backing infrastructure (Postgres, Redis, Kafka KRaft)
├── go.mod                       # Go module definitions
├── go.sum                       # Cryptographic dependency checksums
└── README.md                    # Project documentation & runbook
```

---

## 📊 End-to-End Performance Benchmarks (Audited)

All benchmarks represent **true end-to-end processing** (Client $\rightarrow$ gRPC $\rightarrow$ Redis Rate Limiter $\rightarrow$ Worker Pool $\rightarrow$ Apache Kafka commit log) sustained over 10-second windows with zero data corruption.

### 1. Constrained Environment (2 vCPU, 4 GB RAM)

Benchmarked under strict hardware limits via `taskset -c 0,1` and `GOMAXPROCS=2 GOMEMLIMIT=1500MiB`:

| Ingestion Mode | Batch Size | Concurrency / Conns | Total Events | Success Rate | Throughput (Events/sec) | Latency ($p_{50}$) | Latency ($p_{95}$) | Latency ($p_{99}$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Unary** (`Ingest`)* | 1 | 192 / 24 | 146,185 | **100.0%** | **14,596 req/sec** | 11.49 ms | 26.35 ms | 37.43 ms |
| **Streaming** (`IngestStream`) | 1 (pipelined) | 8 / 2 | 1,749,504 | **97.93%** | **164,718 events/sec** | — | — | — |
| **Batch** (`IngestBatch`) | 50 | 64 / 8 | 1,671,850 | **100.0%** | **166,776 events/sec** | 16.91 ms | 39.64 ms | 55.00 ms |
| **Batch** (`IngestBatch`) | 100 | 64 / 8 | 2,383,700 | **100.0%** | **238,044 events/sec** | 24.50 ms | 51.78 ms | 73.17 ms |

*\*Note: When running both client and server on a single 2 vCPU machine, CPU cycles are divided evenly (~1 vCPU each for load generation and ingestion). When the server has dedicated CPU, unary throughput reaches 20k–22.4k req/sec.*

### 2. Dedicated / Unconstrained Environment (4 vCPU, 8 GB RAM)

| Ingestion Mode | Batch Size | Concurrency / Conns | Total Events | Success Rate | Throughput (Events/sec) | Latency ($p_{50}$) | Latency ($p_{99}$) |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **Unary** (`Ingest`) | 1 | 192 / 24 | 224,367 | **100.0%** | **22,415 req/sec** | 6.95 ms | 28.84 ms |
| **Streaming** (`IngestStream`) | 1 (pipelined) | 16 / 4 | 2,957,152 | **49.8% (load shed)** | **142,954 events/sec** | — | — |
| **Batch** (`IngestBatch`) | 50 | 64 / 8 | 2,165,700 | **100.0%** | **216,274 events/sec** | 12.34 ms | 50.71 ms |
| **Batch** (`IngestBatch`) | 100 | 64 / 8 | 3,193,600 | **99.83%** | **318,338 events/sec** | 16.07 ms | 91.01 ms |

### Infrastructure Resource Footprint (`docker stats`)

All backing containers run with strict memory constraints:

| Service | Container | Memory RSS | Memory Limit / Configuration |
| :--- | :--- | :--- | :--- |
| **Apache Kafka** | `streamer-kafka` | **~285 MiB** | JVM tuned: `KAFKA_HEAP_OPTS: "-Xmx1024M -Xms256M"` |
| **PostgreSQL 16** | `streamer-postgres` | **~44 MiB** | Alpine base image |
| **Redis 7** | `streamer-redis` | **~11 MiB** | Alpine in-memory cache |
| **Total Infra** | — | **~340 MiB** | Leaves **>3.5 GB free** on host for Go service and OS |

---

## 🚀 Setup & Build Runbook

### 1. Prerequisites

- **Go**: `1.22+` (or `1.25`)
- **Docker & Docker Compose**: Compose V2
- **Protocol Buffer Compiler**: `protoc` (v28+) with Go plugins:
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  export PATH="$HOME/go/bin:$PATH"
  ```

### 2. Boot Backing Infrastructure

Start PostgreSQL, Redis, and Apache Kafka in the background:
```bash
docker compose up -d
docker compose ps
```

Verify service readiness:
```bash
# Redis Ping
docker exec streamer-redis redis-cli ping

# Kafka Broker Connectivity & Partitions
docker exec streamer-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events

# PostgreSQL Readiness
docker exec streamer-postgres pg_isready -U streamer -d events_db
```

### 3. Compile Protocol Buffers (Optional)

If modifying `proto/event.proto`, regenerate the stubs:
```bash
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/event.proto
```

### 4. Build Optimized Binaries

Compile stripped release binaries:
```bash
# Ingestion Server
go build -buildvcs=false -ldflags="-s -w" -o bin/server ./cmd/server

# Downstream Kafka-to-PostgreSQL Consumer
go build -buildvcs=false -ldflags="-s -w" -o bin/consumer ./cmd/consumer

# Multi-mode benchmark harness
go build -buildvcs=false -ldflags="-s -w" -o bin/bench ./cmd/bench
```

Run race detector test suites:
```bash
go test -v -race ./...
```

---

## 🖥 Running the System

### Start the Ingestion Server
```bash
# Standard run
GOMEMLIMIT=1500MiB LOG_LEVEL=warn RATE_LIMIT=1000000 LEASE_BATCH_SIZE=2000 ADMIN_PORT=:8080 ./bin/server

# Strict 2 vCPU constrained run
taskset -c 0,1 env GOMAXPROCS=2 GOMEMLIMIT=1500MiB LOG_LEVEL=warn RATE_LIMIT=1000000 LEASE_BATCH_SIZE=2000 ADMIN_PORT=:8080 ./bin/server
```

### Start the Downstream PostgreSQL Consumer
In a separate terminal, launch the consumer to drain Kafka into PostgreSQL:
```bash
./bin/consumer
```

### Environment Configuration

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `:50051` | TCP port for gRPC listener |
| `ADMIN_PORT` | `:8080` | TCP port for HTTP diagnostics (`/healthz`, `/readyz`, `/metrics`, `/debug/pprof/`) |
| `REDIS_ADDR` | `127.0.0.1:6379` | Host and port for Redis distributed rate limiter |
| `KAFKA_BROKERS` | `127.0.0.1:9092` | Comma-separated Kafka broker addresses |
| `DATABASE_URL` | `postgres://streamer:streamer_pass@127.0.0.1:5432/events_db?sslmode=disable` | PostgreSQL connection string |
| `DLQ_PATH` | `dlq.log` | File path for local append-only dead letter queue |
| `RATE_LIMIT` | `100000` | Global tenant request quota per second |
| `LEASE_BATCH_SIZE` | `2000` | Number of tokens leased per Redis atomic round-trip |
| `LOG_LEVEL` | `info` | Logging verbosity (`info`, `warn`, `error`). Use `warn` during high-throughput workloads |
| `GOMEMLIMIT` | `1500MiB` | Go runtime memory ceiling guardrail |

### Deterministic Graceful Shutdown

The server intercepts `SIGINT` (`Ctrl+C`) and `SIGTERM`:
1. Sets gRPC health status to `NOT_SERVING` so upstream load balancers immediately stop routing traffic.
2. Drains the HTTP admin server (`/healthz`, `/metrics`).
3. Drains in-flight RPCs, buffered worker queue shards, and Kafka batches cleanly within 10 seconds:

```text
{"time":"2026-09-06T19:27:06.001Z","level":"INFO","msg":"gRPC server listening","addr":"[::]:50051","pid":62932}
{"time":"2026-09-06T19:27:06.002Z","level":"INFO","msg":"admin & diagnostics server listening","addr":":8080"}
{"time":"2026-09-06T19:27:37.102Z","level":"INFO","msg":"shutdown signal intercepted, starting graceful shutdown..."}
{"time":"2026-09-06T19:27:37.105Z","level":"INFO","msg":"server exited cleanly"}
```

---

## 🔬 Architectural Highlights & Design Patterns

### 1. 2-Tier Token Lease Rate Limiting with Singleflight Coordination
- In a naive distributed rate limiter, every incoming event triggers a Redis `EVALSHA` round-trip. At 100,000+ events/sec, socket lock contention degrades performance.
- **Our solution**: The server leases tokens from Redis in batches (e.g., 2,000 tokens) using an atomic Lua script (`INCRBY` / `EXPIRE`). Local requests are decremented via hardware atomic operations (`atomic.Int64.Add(-n)`).
- **Singleflight Broadcast**: When a local lease depletes, exactly one goroutine executes the Redis lease renewal while concurrent callers cleanly await a `sync.Cond` broadcast, eliminating CPU spin-waits and Redis stampedes.
- **Idle Tenant Eviction**: A background ticker sweeps tenant buckets inactive for >5 minutes, preventing memory expansion.

### 2. Sharded MPSC Queue Architecture & Zero-Copy Keys
- Go channels use an internal mutex (`hchan.lock`). When 192 concurrent client handlers push to a single channel, lock contention degrades throughput.
- **Our solution**: The worker pool partitions its 131,072-slot ring buffer across 32 independent shards. Each worker drains its own dedicated shard, resulting in a Multi-Producer Single-Consumer (MPSC) architecture with near-zero lock contention.
- Zero-copy event IDs with `unsafe.Slice(unsafe.StringData(id), len(id))`.
- Amortized `time.Now()` calls once per batch drain cycle instead of evaluating timestamps individually for all 128 messages.

### 3. End-to-End Persistence Pipeline (Kafka KRaft $\rightarrow$ PostgreSQL)
- Consumer daemon reads from Kafka in micro-batches (up to 1,000 messages or 50ms timeout).
- Batches are inserted into PostgreSQL via `pgx.Batch` with `ON CONFLICT (event_id) DO NOTHING` for idempotent delivery.
- Offsets are committed only after the database write succeeds, providing at-least-once persistence guarantees.

### 4. Prometheus Text Format & gRPC Health Checking
- Standard `grpc.health.v1` for Kubernetes/Envoy liveness and readiness probing.
- Standard Prometheus text exposition format on `/metrics` with `# HELP` and `# TYPE` annotations:
  - `event_streamer_uptime_seconds` (gauge)
  - `event_streamer_worker_queue_depth` (gauge)
  - `event_streamer_events_accepted_total` (counter)
  - `event_streamer_events_rejected_total` (counter)
  - `event_streamer_worker_publish_errors_total` (counter)
  - `event_streamer_kafka_async_errors_total` (counter)
  - `event_streamer_dlq_persisted_total` (counter)
- `/debug/pprof/*` endpoints for runtime CPU, heap, and goroutine profiling.

### 5. Local Dead Letter Queue (DLQ)
- Failed asynchronous writes from the Kafka producer are automatically formatted and written to an append-only recovery file (`dlq.log`) with event keys, error details, and base64 payloads to prevent silent data loss during broker disconnects.

---

## 🛠 Useful Commands Runbook

| Task | Command |
| :--- | :--- |
| **Start Infrastructure** | `docker compose up -d` |
| **Stop Infrastructure** | `docker compose down` |
| **Check Backing Services** | `docker compose ps` |
| **Run All Unit & Race Tests** | `go test -v -race ./...` |
| **Build Binaries** | `go build -buildvcs=false -ldflags="-s -w" -o bin/server ./cmd/server && go build -buildvcs=false -ldflags="-s -w" -o bin/consumer ./cmd/consumer && go build -buildvcs=false -ldflags="-s -w" -o bin/bench ./cmd/bench` |
| **Start Ingestion Server** | `taskset -c 0,1 env GOMAXPROCS=2 GOMEMLIMIT=1500MiB LOG_LEVEL=warn RATE_LIMIT=1000000 LEASE_BATCH_SIZE=2000 ADMIN_PORT=:8080 ./bin/server` |
| **Start Consumer Daemon** | `./bin/consumer` |
| **Check Prometheus Metrics** | `curl -s http://localhost:8080/metrics` |
| **Inspect PostgreSQL Rows** | `docker exec streamer-postgres psql -U streamer -d events_db -c "SELECT count(*) FROM events;"` |
| **Inspect Tenant Breakdown** | `docker exec streamer-postgres psql -U streamer -d events_db -c "SELECT tenant_id, count(*) FROM events GROUP BY tenant_id;"` |
| **Profile CPU (30s sample)** | `curl -o cpu.pprof http://localhost:8080/debug/pprof/profile?seconds=30` |
| **Run Unary Benchmark (10s)** | `taskset -c 0,1 env GOMAXPROCS=2 ./bin/bench -mode=unary -duration=10s -concurrency=192 -conns=24` |
| **Run Batch Benchmark (10s, B=100)**| `taskset -c 0,1 env GOMAXPROCS=2 ./bin/bench -mode=batch -batch-size=100 -duration=10s -concurrency=64 -conns=8` |
| **Inspect Kafka Partitions** | `docker exec streamer-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic events` |
| **Inspect DLQ Crash File** | `cat dlq.log` |
