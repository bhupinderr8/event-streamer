package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"google.golang.org/grpc"
)

// eventServer implements the generated eventv1.EventServiceServer interface.
type eventServer struct {
	eventv1.UnimplementedEventServiceServer
	logger *slog.Logger
}

// Ingest handles incoming ingestion requests, logs event metadata, and returns acknowledgement.
func (s *eventServer) Ingest(ctx context.Context, req *eventv1.IngestRequest) (*eventv1.IngestResponse, error) {
	s.logger.InfoContext(ctx, "event received",
		slog.String("event_id", req.GetEventId()),
		slog.String("tenant_id", req.GetTenantId()),
		slog.Int64("event_timestamp", req.GetTimestamp()),
	)

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
		logger: logger,
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
