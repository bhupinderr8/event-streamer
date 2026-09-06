# Concurrent Event Streaming & Distributed Rate Limiter

A high-throughput, low-latency event ingestion engine and distributed rate limiting system built in Go, designed to operate reliably under constrained hardware environments (e.g., 2 vCPU, 4 GB RAM) while exceeding a **20,000+ req/sec** baseline design target.

---

## 🏛 Architecture & Tech Stack

```
[ Clients ] ──(gRPC / HTTP/2 multiplexing)──▶ [ Event Ingestion Service (:50051) ]
                                                    │
                 ┌──────────────────────────────────┼─────────────────────────────────┐
                 ▼                                  ▼                                 ▼
      [ Redis 7 (:6379) ]                [ Kafka KRaft (:9092) ]          [ PostgreSQL 16 (:5432) ]
   Distributed Rate Limiting             Event Streaming & Log                Persistent Event Store
```

- **Runtime & Language**: Go 1.22+ (Compiled with `-race` verification in test and stripped release binaries for production)
- **Transport**: gRPC / Protocol Buffers (`proto3`)
- **Event Streaming**: Apache Kafka in **KRaft mode** (ZooKeeper-less) with strict JVM heap capping (`-Xmx384M -Xms128M`)
- **Rate Limiting**: Redis 7 Alpine (sliding-window / token-bucket counters)
- **Persistence**: PostgreSQL 16 Alpine with Docker persistent volume (`pgdata`)
- **Memory Guardrails**: `GOMEMLIMIT=1500MiB` aware runtime to prevent Linux OOM-killer panics

---

## 📁 Repository Structure

```text
.
├── cmd/
│   ├── server/
│   │   └── main.go              # Production gRPC server with graceful shutdown & slog JSON logging
│   └── bench/
│       └── main.go              # High-concurrency benchmark harness with latency percentile tracking
├── proto/
│   ├── event.proto              # Canonical protobuf definition (event.v1)
│   ├── event.pb.go              # Protoc-generated message structs
│   └── event_grpc.pb.go         # Protoc-generated gRPC service & client interfaces
├── internal/
│   ├── broker/                  # Kafka producer/consumer implementations & batching
│   ├── limiter/                 # Redis-backed distributed rate limiter
│   ├── storage/                 # PostgreSQL event persistence layer
│   └── workerpool/              # Worker pool for non-blocking asynchronous pipeline execution
├── bin/                         # Compiled Go binaries (bin/server, bin/bench)
├── docker-compose.yml           # Backing infrastructure (Postgres, Redis, Kafka KRaft)
├── go.mod                       # Go module definitions
├── go.sum                       # Cryptographic dependency checksums
└── README.md                    # Project documentation & benchmark runbook
```

---

## ⚙️ Prerequisites

- **Go**: `1.22+`
- **Docker & Docker Compose**: Compose V2
- **Protocol Buffer Compiler**: `protoc` (v28+) with Go plugins:
  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  export PATH="$HOME/go/bin:$PATH"
  ```

---

## 🚀 Setup & Build Runbook

### 1. Boot Backing Infrastructure
Spin up PostgreSQL, Redis, and Apache Kafka in the background:
```bash
docker compose up -d
docker compose ps
```

Verify that all three services are healthy:
```bash
# PostgreSQL
docker exec streamer-postgres pg_isready -U streamer -d events_db

# Redis
docker exec streamer-redis redis-cli ping

# Kafka
docker exec streamer-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 2. Compile Protocol Buffers
Regenerate gRPC and protobuf code from `proto/event.proto`:
```bash
protoc \
  --go_out=. \
  --go_opt=paths=source_relative \
  --go-grpc_out=. \
  --go-grpc_opt=paths=source_relative \
  proto/event.proto
```

### 3. Fetch Dependencies & Compile Binaries
```bash
# Tidy Go dependencies
go mod tidy

# Build production gRPC server binary
go build -ldflags="-s -w" -o bin/server ./cmd/server

# Build high-concurrency benchmark harness
go build -o bin/bench ./cmd/bench
```

> **Note**: For race condition validation during development, compile with `-race`:
> ```bash
> go build -race -o bin/server ./cmd/server
> ```

---

## 🖥 Running the Server

Start the server configured with memory boundaries and JSON logging:

```bash
# Production execution with memory limit guardrail
GOMEMLIMIT=1500MiB LOG_LEVEL=info ./bin/server
```

### Graceful Shutdown
The server intercepts `SIGINT` (`os.Interrupt`) and `SIGTERM` (`syscall.SIGTERM`). In-flight RPCs are drained within a 10-second timeout before shutdown:
```text
{"time":"2026-09-06T10:10:14.522Z","level":"INFO","msg":"gRPC server listening","addr":"[::]:50051","pid":18}
{"time":"2026-09-06T10:10:15.460Z","level":"INFO","msg":"shutdown signal intercepted, starting graceful shutdown..."}
{"time":"2026-09-06T10:10:15.461Z","level":"INFO","msg":"server exited cleanly"}
```

---

## 📊 Performance Benchmarks (29,500 req/sec)

### Environment Specifications
- **Host**: Ubuntu Linux VM (2 vCPU, 4 GB RAM)
- **Workload**: gRPC `Ingest` RPC handling structured JSON payloads
- **Load Harness**: `cmd/bench` with 128 concurrent worker goroutines multiplexed across 16 gRPC TCP connections

### Backing Infrastructure Footprint (`docker stats`)
All backing services run with strict memory bounds to prevent host OOM:

| Service | Container | Status | Memory RSS | Memory Cap / Constraint |
| :--- | :--- | :--- | :--- | :--- |
| **Apache Kafka** | `streamer-kafka` | Up | **~306.9 MiB** | JVM capped at `-Xmx384M -Xms128M` |
| **PostgreSQL 16**| `streamer-postgres`| Up | **~43.4 MiB** | Alpine base image |
| **Redis 7** | `streamer-redis` | Up | **~12.5 MiB** | Alpine in-memory cache |
| **Total Infra** | — | — | **~362.8 MiB** | Leaves >3 GB free on 4 GB host |

### Benchmark Results
Executed a sustained 10-second load test with `LOG_LEVEL=warn` to measure raw network and runtime serialization performance:

```text
================= BENCHMARK RESULTS =================
Duration:         10.01 s
Total Requests:   295,161
Successful:       295,161
Errors:           0 (0.00%)
Throughput:       29,500.21 req/sec
Latency (p50):    3.62 ms
Latency (p95):    9.74 ms
Latency (p99):    14.40 ms
Latency (max):    36.66 ms
=====================================================
```

### Key Architectural Takeaways
1. **Surpassed Target**: Exceeded the 20,000 req/sec goal by delivering **29,500 req/sec** with **zero error rate**.
2. **Predictable Latency Profile**: Sub-4ms median latency (`p50 = 3.62ms`) and tight tail latency (`p99 = 14.40ms`).
3. **I/O Locking Avoidance**: Under 20k+ req/sec loads, synchronous stdout logging introduces severe mutex contention on standard I/O streams. The application uses configurable log levels (`LOG_LEVEL`) and asynchronous logging patterns for production workloads.
4. **Predictable Memory Profile**: With `GOMEMLIMIT=1500MiB`, the Go runtime's garbage collector automatically scales heap pacing to ensure the application stays within container memory ceilings.

---

## 🛠 Useful Commands

| Task | Command |
| :--- | :--- |
| **Start Infrastructure** | `docker compose up -d` |
| **Stop Infrastructure** | `docker compose down` |
| **Reset Data Volumes** | `docker compose down -v` |
| **View Infrastructure Logs** | `docker compose logs -f` |
| **Run Load Benchmark (5s)** | `./bin/bench -duration=5s -concurrency=64 -conns=8` |
| **Run Sustained Benchmark (10s)** | `./bin/bench -duration=10s -concurrency=128 -conns=16` |
| **Check Port Binding** | `ss -tulpn \| grep 50051` |

