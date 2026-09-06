package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bhupinder121199/event-streamer/internal/broker"
	"github.com/bhupinder121199/event-streamer/internal/limiter"
	"github.com/bhupinder121199/event-streamer/internal/workerpool"
	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"google.golang.org/grpc"
)

// eventServer implements the generated eventv1.EventServiceServer interface.
type eventServer struct {
	eventv1.UnimplementedEventServiceServer
	logger  *slog.Logger
	limiter limiter.Limiter
	pool    *workerpool.WorkerPool
}

// Ingest handles incoming ingestion requests, checks rate limits, submits to worker pool, and returns acknowledgement.
func (s *eventServer) Ingest(ctx context.Context, req *eventv1.IngestRequest) (*eventv1.IngestResponse, error) {
	// 1. Check rate limit
	allowed, err := s.limiter.Allow(ctx, req.GetTenantId())
	if err != nil {
		s.logger.ErrorContext(ctx, "rate limiter check error",
			slog.String("tenant_id", req.GetTenantId()),
			slog.String("error", err.Error()),
		)
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "internal rate limiter error",
			ProcessedAt: time.Now().UnixNano(),
		}, nil
	}

	if !allowed {
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "rate limit exceeded",
			ProcessedAt: time.Now().UnixNano(),
		}, nil
	}

	// 2. Submit event to worker pool buffer
	if !s.pool.Submit(req) {
		s.logger.WarnContext(ctx, "worker pool buffer saturated; shedding load",
			slog.String("event_id", req.GetEventId()),
		)
		return &eventv1.IngestResponse{
			Accepted:    false,
			Message:     "buffer full; shed load",
			ProcessedAt: time.Now().UnixNano(),
		}, nil
	}

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
		ProcessedAt: time.Now().UnixNano(),
	}, nil
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

	rateLimiter, err := limiter.NewRedisLimiter(limiter.Config{
		RedisAddr:      redisAddr,
		DefaultLimit:   defaultLimit,
		Window:         time.Second,
		LeaseBatchSize: 500,
	})
	if err != nil {
		logger.Error("failed to connect to redis rate limiter", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer rateLimiter.Close()

	// Kafka producer setup
	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "127.0.0.1:9092"
	}
	kafkaProducer := broker.NewKafkaProducer(broker.Config{
		Brokers:      []string{kafkaBrokers},
		DefaultTopic: "events",
		BatchSize:    5000,
		BatchTimeout: 5 * time.Millisecond,
		Async:        true,
	})
	defer kafkaProducer.Close()

	// Worker pool setup
	pool := workerpool.New(kafkaProducer, 32, 100000)
	defer pool.Stop()

	const port = ":50051"
	lis, err := net.Listen("tcp", port)
	if err != nil {
		logger.Error("failed to bind tcp listener",
			slog.String("port", port),
			slog.String("error", err.Error()),
		)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	srv := &eventServer{
		logger:  logger,
		limiter: rateLimiter,
		pool:    pool,
	}
	eventv1.RegisterEventServiceServer(grpcServer, srv)

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
