package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"sort"
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
	concurrency := flag.Int("concurrency", 64, "number of concurrent worker goroutines")
	connections := flag.Int("conns", 8, "number of multiplexed TCP gRPC connections")
	flag.Parse()

	log.Printf("Starting benchmark against %s for %v (concurrency=%d, conns=%d)...",
		*targetAddr, *duration, *concurrency, *connections)

	// Establish pooled gRPC client connections
	clients := make([]eventv1.EventServiceClient, *connections)
	connList := make([]*grpc.ClientConn, *connections)
	for i := 0; i < *connections; i++ {
		conn, err := grpc.NewClient(*targetAddr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024)),
		)
		if err != nil {
			log.Fatalf("failed to connect to gRPC server: %v", err)
		}
		defer conn.Close()
		connList[i] = conn
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

	var totalSuccess int64
	var totalErrors int64

	type workerStats struct {
		latencies []time.Duration
	}
	allStats := make([]workerStats, *concurrency)

	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	startTime := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			client := clients[workerIdx%len(clients)]
			stats := &allStats[workerIdx]
			stats.latencies = make([]time.Duration, 0, 100000)

			req := &eventv1.IngestRequest{
				EventId:   "evt-bench",
				TenantId:  "tenant-alpha",
				Payload:   `{"action":"click","category":"electronics","value":49.99}`,
				Timestamp: time.Now().UnixNano(),
			}

			for {
				select {
				case <-stopChan:
					return
				default:
				}

				callStart := time.Now()
				resp, err := client.Ingest(context.Background(), req)
				elapsed := time.Since(callStart)

				if err != nil || (resp != nil && !resp.Accepted) {
					atomic.AddInt64(&totalErrors, 1)
				} else {
					atomic.AddInt64(&totalSuccess, 1)
					if len(stats.latencies) < cap(stats.latencies) {
						stats.latencies = append(stats.latencies, elapsed)
					}
				}
			}
		}(w)
	}

	// Run for the designated test duration
	time.Sleep(*duration)
	close(stopChan)
	wg.Wait()
	totalElapsed := time.Since(startTime)

	// Aggregate metrics
	successCount := atomic.LoadInt64(&totalSuccess)
	errorCount := atomic.LoadInt64(&totalErrors)
	totalReqs := successCount + errorCount
	rps := float64(successCount) / totalElapsed.Seconds()

	var allLatencies []time.Duration
	for _, s := range allStats {
		allLatencies = append(allLatencies, s.latencies...)
	}
	sort.Slice(allLatencies, func(i, j int) bool {
		return allLatencies[i] < allLatencies[j]
	})

	var p50, p95, p99, maxLat time.Duration
	if len(allLatencies) > 0 {
		p50 = allLatencies[int(float64(len(allLatencies))*0.50)]
		p95 = allLatencies[int(float64(len(allLatencies))*0.95)]
		p99 = allLatencies[int(float64(len(allLatencies))*0.99)]
		maxLat = allLatencies[len(allLatencies)-1]
	}

	fmt.Println("\n================= BENCHMARK RESULTS =================")
	fmt.Printf("Duration:         %.2f s\n", totalElapsed.Seconds())
	fmt.Printf("Total Requests:   %d\n", totalReqs)
	fmt.Printf("Successful:       %d\n", successCount)
	fmt.Printf("Errors:           %d (%.2f%%)\n", errorCount, float64(errorCount)/float64(totalReqs)*100)
	fmt.Printf("Throughput:       %.2f req/sec\n", rps)
	fmt.Printf("Latency (p50):    %v\n", p50)
	fmt.Printf("Latency (p95):    %v\n", p95)
	fmt.Printf("Latency (p99):    %v\n", p99)
	fmt.Printf("Latency (max):    %v\n", maxLat)
	fmt.Println("=====================================================")
}
