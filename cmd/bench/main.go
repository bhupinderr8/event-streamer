package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	targetAddr := flag.String("addr", "localhost:50051", "target gRPC address")
	duration := flag.Duration("duration", 10*time.Second, "benchmark test duration")
	concurrency := flag.Int("concurrency", 192, "number of concurrent worker goroutines")
	connections := flag.Int("conns", 24, "number of multiplexed TCP gRPC connections")
	mode := flag.String("mode", "unary", "benchmark mode: unary, batch, stream")
	batchSize := flag.Int("batch-size", 100, "number of events per batch (batch mode only)")
	flag.Parse()

	log.Printf("Starting benchmark [%s mode] against %s for %v (concurrency=%d, conns=%d, batch_size=%d)...",
		*mode, *targetAddr, *duration, *concurrency, *connections, *batchSize)

	// Establish pooled gRPC client connections with optimized buffer and flow-control settings
	clients := make([]eventv1.EventServiceClient, *connections)
	for i := 0; i < *connections; i++ {
		conn, err := grpc.NewClient(*targetAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithInitialWindowSize(1<<20),
			grpc.WithInitialConnWindowSize(2<<20),
			grpc.WithReadBufferSize(64*1024),
			grpc.WithWriteBufferSize(64*1024),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(10*1024*1024)),
		)
		if err != nil {
			log.Fatalf("failed to connect to gRPC server: %v", err)
		}
		defer conn.Close()
		clients[i] = eventv1.NewEventServiceClient(conn)
	}

	// Warm-up ping to verify connectivity
	warmupCtx, warmupCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer warmupCancel()
	_, err := clients[0].Ingest(warmupCtx, &eventv1.IngestRequest{
		EventId:   "warmup-1",
		TenantId:  "warmup-tenant",
		Payload:   "warmup",
		Timestamp: time.Now().UnixNano(),
	})
	if err != nil {
		log.Fatalf("warm-up RPC failed: %v", err)
	}

	var totalRPCSuccess int64
	var totalRPCErrors int64
	var totalEventsSuccess int64
	var totalEventsErrors int64

	type workerStats struct {
		latencies []time.Duration
	}
	allStats := make([]workerStats, *concurrency)

	var stopped atomic.Bool
	var wg sync.WaitGroup

	startTime := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			client := clients[workerIdx%len(clients)]
			stats := &allStats[workerIdx]
			stats.latencies = make([]time.Duration, 0, 10000)

			tenantID := fmt.Sprintf("tenant-%d", workerIdx%10)

			switch *mode {
			case "batch":
				events := make([]*eventv1.IngestRequest, *batchSize)
				for i := 0; i < *batchSize; i++ {
					events[i] = &eventv1.IngestRequest{
						EventId:   fmt.Sprintf("evt-b-%d-%d", workerIdx, i),
						TenantId:  tenantID,
						Payload:   `{"action":"click","category":"electronics","value":49.99}`,
						Timestamp: time.Now().UnixNano(),
					}
				}
				batchReq := &eventv1.IngestBatchRequest{
					TenantId: tenantID,
					Events:   events,
				}

				var localRPCSuccess, localRPCErrors int64
				var localEventsSuccess, localEventsErrors int64
				var iter int

				for !stopped.Load() {
					iter++
					if iter%10 == 0 {
						callStart := time.Now()
						resp, err := client.IngestBatch(context.Background(), batchReq)
						elapsed := time.Since(callStart)

						if err != nil || (resp != nil && !resp.Accepted) {
							localRPCErrors++
							localEventsErrors += int64(*batchSize)
						} else {
							localRPCSuccess++
							localEventsSuccess += int64(resp.AcceptedCount)
							localEventsErrors += int64(resp.RejectedCount)
							if len(stats.latencies) < cap(stats.latencies) {
								stats.latencies = append(stats.latencies, elapsed)
							}
						}
					} else {
						resp, err := client.IngestBatch(context.Background(), batchReq)
						if err != nil || (resp != nil && !resp.Accepted) {
							localRPCErrors++
							localEventsErrors += int64(*batchSize)
						} else {
							localRPCSuccess++
							localEventsSuccess += int64(resp.AcceptedCount)
							localEventsErrors += int64(resp.RejectedCount)
						}
					}
				}

				atomic.AddInt64(&totalRPCSuccess, localRPCSuccess)
				atomic.AddInt64(&totalRPCErrors, localRPCErrors)
				atomic.AddInt64(&totalEventsSuccess, localEventsSuccess)
				atomic.AddInt64(&totalEventsErrors, localEventsErrors)

			case "stream":
				req := &eventv1.IngestRequest{
					EventId:   fmt.Sprintf("evt-stream-%d", workerIdx),
					TenantId:  tenantID,
					Payload:   `{"action":"click","category":"electronics","value":49.99}`,
					Timestamp: time.Now().UnixNano(),
				}

				stream, err := client.IngestStream(context.Background())
				if err != nil {
					atomic.AddInt64(&totalRPCErrors, 1)
					return
				}

				var streamEvents int64
				for !stopped.Load() {
					if err := stream.Send(req); err != nil {
						if err == io.EOF {
							break
						}
						atomic.AddInt64(&totalEventsErrors, 1)
						break
					}
					streamEvents++
				}

				resp, err := stream.CloseAndRecv()
				if err != nil {
					atomic.AddInt64(&totalRPCErrors, 1)
					atomic.AddInt64(&totalEventsErrors, streamEvents)
				} else if resp != nil {
					atomic.AddInt64(&totalRPCSuccess, 1)
					atomic.AddInt64(&totalEventsSuccess, resp.AcceptedCount)
					atomic.AddInt64(&totalEventsErrors, resp.RejectedCount)
				}

			default: // "unary"
				req := &eventv1.IngestRequest{
					EventId:   fmt.Sprintf("evt-unary-%d", workerIdx),
					TenantId:  tenantID,
					Payload:   `{"action":"click","category":"electronics","value":49.99}`,
					Timestamp: time.Now().UnixNano(),
				}

				var localSuccess, localErrors int64
				var iter int

				for !stopped.Load() {
					iter++
					if iter%10 == 0 {
						callStart := time.Now()
						resp, err := client.Ingest(context.Background(), req)
						elapsed := time.Since(callStart)

						if err != nil || (resp != nil && !resp.Accepted) {
							localErrors++
						} else {
							localSuccess++
							if len(stats.latencies) < cap(stats.latencies) {
								stats.latencies = append(stats.latencies, elapsed)
							}
						}
					} else {
						resp, err := client.Ingest(context.Background(), req)
						if err != nil || (resp != nil && !resp.Accepted) {
							localErrors++
						} else {
							localSuccess++
						}
					}
				}

				atomic.AddInt64(&totalRPCSuccess, localSuccess)
				atomic.AddInt64(&totalRPCErrors, localErrors)
				atomic.AddInt64(&totalEventsSuccess, localSuccess)
				atomic.AddInt64(&totalEventsErrors, localErrors)
			}
		}(w)
	}

	// Run for the designated test duration
	time.Sleep(*duration)
	stopped.Store(true)
	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Aggregate metrics
	successRPCs := atomic.LoadInt64(&totalRPCSuccess)
	errorRPCs := atomic.LoadInt64(&totalRPCErrors)
	totalRPCs := successRPCs + errorRPCs

	successEvents := atomic.LoadInt64(&totalEventsSuccess)
	errorEvents := atomic.LoadInt64(&totalEventsErrors)
	totalEvents := successEvents + errorEvents

	eventsPerSec := float64(successEvents) / totalElapsed.Seconds()
	rpcsPerSec := float64(successRPCs) / totalElapsed.Seconds()

	var allLatencies []time.Duration
	for _, s := range allStats {
		allLatencies = append(allLatencies, s.latencies...)
	}
	slices.Sort(allLatencies)

	var p50, p95, p99, maxLat time.Duration
	if len(allLatencies) > 0 {
		p50 = allLatencies[int(float64(len(allLatencies))*0.50)]
		p95 = allLatencies[int(float64(len(allLatencies))*0.95)]
		p99 = allLatencies[int(float64(len(allLatencies))*0.99)]
		maxLat = allLatencies[len(allLatencies)-1]
	}

	errPct := 0.0
	if totalEvents > 0 {
		errPct = float64(errorEvents) / float64(totalEvents) * 100
	}

	fmt.Println("\n================= BENCHMARK RESULTS =================")
	fmt.Printf("Benchmark Mode:   %s\n", *mode)
	fmt.Printf("Duration:         %.2f s\n", totalElapsed.Seconds())
	fmt.Printf("Total RPCs:       %d (%.2f rpc/sec)\n", totalRPCs, rpcsPerSec)
	fmt.Printf("Total Events:     %d\n", totalEvents)
	fmt.Printf("Successful:       %d\n", successEvents)
	fmt.Printf("Errors:           %d (%.2f%%)\n", errorEvents, errPct)
	fmt.Printf("Throughput:       %.2f events/sec\n", eventsPerSec)
	if len(allLatencies) > 0 {
		fmt.Printf("Latency (p50):    %v\n", p50)
		fmt.Printf("Latency (p95):    %v\n", p95)
		fmt.Printf("Latency (p99):    %v\n", p99)
		fmt.Printf("Latency (max):    %v\n", maxLat)
	}
	fmt.Println("=====================================================")
}
