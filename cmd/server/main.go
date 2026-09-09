package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bhupinder121199/event-streamer/internal/broker"
	"github.com/bhupinder121199/event-streamer/internal/limiter"
	"github.com/bhupinder121199/event-streamer/internal/workerpool"
	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/keepalive"
)

var cachedUnixNano atomic.Int64

func init() {
	cachedUnixNano.Store(time.Now().UnixNano())
	go func() {
		ticker := time.NewTicker(500 * time.Microsecond)
		defer ticker.Stop()
		for t := range ticker.C {
			cachedUnixNano.Store(t.UnixNano())
		}
	}()
}

// eventServer implements the generated eventv1.EventServiceServer interface.
type eventServer struct {
	eventv1.UnimplementedEventServiceServer
	logger        *slog.Logger
	limiter       limiter.Limiter
	pool          *workerpool.WorkerPool
	acceptedCount atomic.Uint64
	rejectedCount atomic.Uint64
}

// Ingest handles incoming ingestion requests, checks rate limits, submits to worker pool, and returns acknowledgement.
func (s *eventServer) Ingest(ctx context.Context, req *eventv1.IngestRequest) (*eventv1.IngestResponse, error) {
	nowNano := cachedUnixNano.Load()

	// 1. Check rate limit
	allowed, err := s.limiter.Allow(ctx, req.GetTenantId())
	if err != nil {
		s.rejectedCount.Add(1)
		s.logger.ErrorContext(ctx, "rate limiter check error",
			slog.String("tenant_id", req.GetTenantId()),
			slog.String("error", err.Error()),
		)
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "internal rate limiter error",
			ProcessedAt: nowNano,
		}, nil
	}

	if !allowed {
		s.rejectedCount.Add(1)
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "rate limit exceeded",
			ProcessedAt: nowNano,
		}, nil
	}

	// 2. Submit event to worker pool buffer
	if !s.pool.Submit(req) {
		s.rejectedCount.Add(1)
		s.logger.WarnContext(ctx, "worker pool buffer saturated; shedding load",
			slog.String("event_id", req.GetEventId()),
		)
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "buffer full; shed load",
			ProcessedAt: nowNano,
		}, nil
	}

	s.acceptedCount.Add(1)

	if s.logger.Enabled(ctx, slog.LevelInfo) {
		s.logger.InfoContext(ctx, "event received and queued",
			slog.String("event_id", req.GetEventId()),
			slog.String("tenant_id", req.GetTenantId()),
			slog.Int64("event_timestamp", req.GetTimestamp()),
		)
	}

	return &eventv1.IngestResponse{
		Accepted:    true,
		Message:     "event queued successfully",
		ProcessedAt: nowNano,
	}, nil
}

// IngestBatch handles batch ingestion requests, reserves quota via AllowN, and enqueues events.
func (s *eventServer) IngestBatch(ctx context.Context, req *eventv1.IngestBatchRequest) (*eventv1.IngestBatchResponse, error) {
	nowNano := cachedUnixNano.Load()
	events := req.GetEvents()
	count := int64(len(events))
	if count == 0 {
		return &eventv1.IngestBatchResponse{
			Accepted:      true,
			AcceptedCount: 0,
			RejectedCount: 0,
			Message:       "empty batch",
			ProcessedAt:   nowNano,
		}, nil
	}

	tenantID := req.GetTenantId()
	if tenantID == "" && len(events) > 0 {
		tenantID = events[0].GetTenantId()
	}

	// 1. Check rate limit for batch
	allowed, err := s.limiter.AllowN(ctx, tenantID, count)
	if err != nil {
		s.rejectedCount.Add(uint64(count))
		s.logger.ErrorContext(ctx, "batch rate limiter check error",
			slog.String("tenant_id", tenantID),
			slog.String("error", err.Error()),
		)
		return &eventv1.IngestBatchResponse{
			Accepted:      false,
			AcceptedCount: 0,
			RejectedCount: int32(count),
			Message:       "internal rate limiter error",
			ProcessedAt:   nowNano,
		}, nil
	}

	if !allowed {
		s.rejectedCount.Add(uint64(count))
		return &eventv1.IngestBatchResponse{
			Accepted:      false,
			AcceptedCount: 0,
			RejectedCount: int32(count),
			Message:       "rate limit exceeded",
			ProcessedAt:   nowNano,
		}, nil
	}

	// 2. Submit events to worker pool buffer
	accepted, allAccepted := s.pool.SubmitBatch(events)
	rejected := int(count) - accepted
	s.acceptedCount.Add(uint64(accepted))
	s.rejectedCount.Add(uint64(rejected))

	if !allAccepted {
		s.logger.WarnContext(ctx, "worker pool buffer saturated during batch ingestion",
			slog.Int("total", int(count)),
			slog.Int("accepted", accepted),
			slog.Int("rejected", rejected),
		)
	}

	return &eventv1.IngestBatchResponse{
		Accepted:      allAccepted,
		AcceptedCount: int32(accepted),
		RejectedCount: int32(rejected),
		Message:       "batch processed",
		ProcessedAt:   nowNano,
	}, nil
}

// IngestStream handles continuous streaming event ingestion over a single gRPC stream.
func (s *eventServer) IngestStream(stream eventv1.EventService_IngestStreamServer) error {
	ctx := stream.Context()
	var totalReceived, acceptedCount, rejectedCount int64

	for {
		req, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			nowNano := cachedUnixNano.Load()
			s.acceptedCount.Add(uint64(acceptedCount))
			s.rejectedCount.Add(uint64(rejectedCount))
			return stream.SendAndClose(&eventv1.IngestStreamResponse{
				Accepted:      acceptedCount > 0,
				TotalReceived: totalReceived,
				AcceptedCount: acceptedCount,
				RejectedCount: rejectedCount,
				Message:       "stream ingestion completed",
				ProcessedAt:   nowNano,
			})
		}
		if err != nil {
			s.acceptedCount.Add(uint64(acceptedCount))
			s.rejectedCount.Add(uint64(rejectedCount))
			return err
		}

		totalReceived++

		allowed, err := s.limiter.Allow(ctx, req.GetTenantId())
		if err != nil || !allowed {
			rejectedCount++
			continue
		}

		if s.pool.Submit(req) {
			acceptedCount++
		} else {
			rejectedCount++
		}
	}
}

func main() {
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "warn" {
		logLevel = slog.LevelWarn
	} else if os.Getenv("LOG_LEVEL") == "error" {
		logLevel = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))
	slog.SetDefault(logger)

	// Rate limiter setup
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "127.0.0.1:6379"
	}
	defaultLimit := int64(100000)
	if limitEnv := os.Getenv("RATE_LIMIT"); limitEnv != "" {
		if parsed, err := strconv.ParseInt(limitEnv, 10, 64); err == nil && parsed > 0 {
			defaultLimit = parsed
		}
	}

	leaseBatchSize := int64(2000)
	if batchEnv := os.Getenv("LEASE_BATCH_SIZE"); batchEnv != "" {
		if parsed, err := strconv.ParseInt(batchEnv, 10, 64); err == nil && parsed > 0 {
			leaseBatchSize = parsed
		}
	}

	rateLimiter, err := limiter.NewRedisLimiter(limiter.Config{
		RedisAddr:      redisAddr,
		DefaultLimit:   defaultLimit,
		Window:         time.Second,
		LeaseBatchSize: leaseBatchSize,
	})
	if err != nil {
		logger.Error("failed to connect to redis rate limiter", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer rateLimiter.Close()

	// Kafka producer setup with multi-partition support and DLQ file fallback
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "127.0.0.1:9092"
	}
	dlqPath := os.Getenv("DLQ_PATH")
	if dlqPath == "" {
		dlqPath = "dlq.log"
	}

	kafkaProducer := broker.NewKafkaProducer(broker.Config{
		Brokers:      []string{kafkaBrokers},
		DefaultTopic: "events",
		BatchSize:    5000,
		BatchTimeout: 5 * time.Millisecond,
		Async:        true,
		DLQPath:      dlqPath,
	})
	defer kafkaProducer.Close()

	// Worker pool setup
	pool := workerpool.New(kafkaProducer, 32, 131072)
	defer pool.Stop()

	port := os.Getenv("PORT")
	if port == "" {
		port = ":50051"
	}
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Error("failed to bind tcp listener",
			slog.String("port", port),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	// Hardened gRPC server options with keepalive enforcement and 16MB envelope
	keepaliveParams := keepalive.ServerParameters{
		MaxConnectionIdle:     15 * time.Minute,
		MaxConnectionAge:      2 * time.Hour,
		MaxConnectionAgeGrace: 5 * time.Minute,
		Time:                  2 * time.Hour,
		Timeout:               20 * time.Second,
	}
	keepaliveEnforcement := keepalive.EnforcementPolicy{
		MinTime:             5 * time.Second,
		PermitWithoutStream: true,
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024),
		grpc.MaxSendMsgSize(16*1024*1024),
		grpc.KeepaliveParams(keepaliveParams),
		grpc.KeepaliveEnforcementPolicy(keepaliveEnforcement),
		grpc.InitialWindowSize(1<<20),
		grpc.InitialConnWindowSize(2<<20),
		grpc.ReadBufferSize(64*1024),
		grpc.WriteBufferSize(64*1024),
	)
	srv := &eventServer{
		logger:  logger,
		limiter: rateLimiter,
		pool:    pool,
	}
	eventv1.RegisterEventServiceServer(grpcServer, srv)

	// Register standard gRPC Health Check service
	healthServer := health.NewServer()
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	healthServer.SetServingStatus("event.v1.EventService", grpc_health_v1.HealthCheckResponse_SERVING)

	// Setup HTTP Admin & Diagnostics Server (:8080)
	adminPort := os.Getenv("ADMIN_PORT")
	if adminPort == "" {
		adminPort = ":8080"
	}
	serverStartTime := time.Now()

	adminMux := http.NewServeMux()
	adminMux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, `{"status":"ok","uptime_sec":%.1f}`+"\n", time.Since(serverStartTime).Seconds())
	})
	adminMux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready\n"))
	})
	adminMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		uptime := time.Since(serverStartTime).Seconds()
		fmt.Fprintf(w, `# HELP event_streamer_uptime_seconds Total server uptime in seconds
# TYPE event_streamer_uptime_seconds gauge
event_streamer_uptime_seconds %.2f

# HELP event_streamer_worker_queue_depth Current number of events queued across worker shards
# TYPE event_streamer_worker_queue_depth gauge
event_streamer_worker_queue_depth %d

# HELP event_streamer_events_accepted_total Total number of events accepted into the ingestion pipeline
# TYPE event_streamer_events_accepted_total counter
event_streamer_events_accepted_total %d

# HELP event_streamer_events_rejected_total Total number of events rejected by rate limiter or queue saturation
# TYPE event_streamer_events_rejected_total counter
event_streamer_events_rejected_total %d

# HELP event_streamer_worker_publish_errors_total Total number of batch publish failures from worker pool
# TYPE event_streamer_worker_publish_errors_total counter
event_streamer_worker_publish_errors_total %d

# HELP event_streamer_kafka_async_errors_total Total number of asynchronous write failures from Kafka writer
# TYPE event_streamer_kafka_async_errors_total counter
event_streamer_kafka_async_errors_total %d

# HELP event_streamer_dlq_persisted_total Total number of messages written to the dead letter queue
# TYPE event_streamer_dlq_persisted_total counter
event_streamer_dlq_persisted_total %d
`,
			uptime,
			pool.QueueDepth(),
			srv.acceptedCount.Load(),
			srv.rejectedCount.Load(),
			pool.PublishErrors(),
			kafkaProducer.AsyncErrors(),
			kafkaProducer.DLQCount(),
		)
	})
	// Mount standard pprof handlers for diagnostics
	adminMux.HandleFunc("/debug/pprof/", pprof.Index)
	adminMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	adminMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	adminMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	adminMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	adminServer := &http.Server{
		Addr:              adminPort,
		Handler:           adminMux,
		ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout:       5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}

	go func() {
		logger.Info("admin & diagnostics server listening", slog.String("addr", adminPort))
		if err := adminServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Warn("admin server stopped", slog.String("error", err.Error()))
		}
	}()

	// Setup signal context for deterministic graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		logger.Info("gRPC server listening",
			slog.String("addr", lis.Addr().String()),
			slog.Int("pid", os.Getpid()),
		)
		if err := grpcServer.Serve(lis); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			serverErr <- err
		}
		close(serverErr)
	}()

	// Block until an OS termination signal is intercepted or server encounters a fatal error
	select {
	case err := <-serverErr:
		if err != nil {
			logger.Error("gRPC server failed unexpectedly", slog.String("error", err.Error()))
			os.Exit(1)
		}
	case <-ctx.Done():
		logger.Info("shutdown signal intercepted, starting graceful shutdown...")

		// Mark gRPC health status as NOT_SERVING so external load balancers stop sending new requests
		healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_NOT_SERVING)
		healthServer.SetServingStatus("event.v1.EventService", grpc_health_v1.HealthCheckResponse_NOT_SERVING)

		// Stop HTTP admin server
		adminCtx, adminCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer adminCancel()
		_ = adminServer.Shutdown(adminCtx)

		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()

		select {
		case <-stopped:
			logger.Info("server exited cleanly")
		case <-time.After(10 * time.Second):
			logger.Warn("graceful shutdown timed out; forcing abrupt stop")
			grpcServer.Stop()
		}
	}
}
