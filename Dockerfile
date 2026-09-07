# Stage 1: Shared compilation stage
FROM golang:alpine AS builder

WORKDIR /app

# Cache dependency layer
COPY go.mod go.sum ./
RUN go mod download

# Copy source tree and compile pure Go static binaries
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -ldflags="-s -w" -o /out/server ./cmd/server && \
    CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -ldflags="-s -w" -o /out/consumer ./cmd/consumer && \
    CGO_ENABLED=0 GOOS=linux go build -buildvcs=false -ldflags="-s -w" -o /out/bench ./cmd/bench

# Stage 2: Production Ingestion Server
FROM alpine:3.21 AS server
WORKDIR /app
COPY --from=builder /out/server /app/server
EXPOSE 50051 8080
ENTRYPOINT ["/app/server"]

# Stage 3: Downstream Kafka-to-PostgreSQL Consumer Daemon
FROM alpine:3.21 AS consumer
WORKDIR /app
COPY --from=builder /out/consumer /app/consumer
ENTRYPOINT ["/app/consumer"]

# Stage 4: Multi-mode Benchmark Harness
FROM alpine:3.21 AS bench
WORKDIR /app
COPY --from=builder /out/bench /app/bench
ENTRYPOINT ["/app/bench"]
CMD ["-addr=server:50051", "-mode=unary", "-duration=10s"]

