package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/bhupinder121199/event-streamer/internal/storage"
	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	kafkaBrokers := os.Getenv("KAFKA_BROKERS")
	if kafkaBrokers == "" {
		kafkaBrokers = "127.0.0.1:9092"
	}
	topic := os.Getenv("KAFKA_TOPIC")
	if topic == "" {
		topic = "events"
	}
	groupID := os.Getenv("KAFKA_GROUP_ID")
	if groupID == "" {
		groupID = "event-streamer-consumers"
	}
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		logger.Error("DATABASE_URL environment variable is required")
		os.Exit(1)
	}

	batchSize := 1000
	if bEnv := os.Getenv("BATCH_SIZE"); bEnv != "" {
		if parsed, err := strconv.Atoi(bEnv); err == nil && parsed > 0 {
			batchSize = parsed
		}
	}
	batchTimeout := 50 * time.Millisecond
	if tEnv := os.Getenv("BATCH_TIMEOUT_MS"); tEnv != "" {
		if parsed, err := strconv.Atoi(tEnv); err == nil && parsed > 0 {
			batchTimeout = time.Duration(parsed) * time.Millisecond
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("initializing postgresql storage layer")
	store, err := storage.NewPostgresStorage(ctx, dbURL)
	if err != nil {
		logger.Error("failed to connect to postgresql", slog.String("error", err.Error()))
		os.Exit(1)
	}
	defer store.Close()

	logger.Info("initializing kafka consumer reader",
		slog.String("brokers", kafkaBrokers),
		slog.String("topic", topic),
		slog.String("group_id", groupID),
		slog.Int("batch_size", batchSize),
	)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokers},
		GroupID:        groupID,
		Topic:          topic,
		MinBytes:       10e3, // 10KB
		MaxBytes:       10e6, // 10MB
		MaxWait:        batchTimeout,
		CommitInterval: 0, // synchronous manual commit upon DB transaction completion
		QueueCapacity:  10000,
	})
	defer reader.Close()

	logger.Info("kafka consumer daemon running; listening for events...")

	eventsBatch := make([]*eventv1.IngestRequest, 0, batchSize)
	messagesBatch := make([]kafka.Message, 0, batchSize)
	flushTicker := time.NewTicker(batchTimeout)
	defer flushTicker.Stop()

	var totalPersisted int64

	flush := func() {
		if len(eventsBatch) == 0 {
			return
		}

		flushStart := time.Now()
		insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := store.InsertBatch(insertCtx, eventsBatch)
		cancel()

		if err != nil {
			logger.Error("failed to batch-insert into postgresql",
				slog.Int("batch_len", len(eventsBatch)),
				slog.String("error", err.Error()),
			)
			// Batch retained for retry on next cycle (offsets not committed)
			return
		}

		// Commit Kafka offsets only after successful DB persistence
		commitCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = reader.CommitMessages(commitCtx, messagesBatch...)
		cancel()

		if err != nil {
			logger.Warn("kafka commit offsets warning", slog.String("error", err.Error()))
		}

		totalPersisted += int64(len(eventsBatch))
		logger.Info("persisted batch to postgresql",
			slog.Int("batch_size", len(eventsBatch)),
			slog.Duration("latency", time.Since(flushStart)),
			slog.Int64("total_persisted", totalPersisted),
		)

		eventsBatch = eventsBatch[:0]
		messagesBatch = messagesBatch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			logger.Info("shutdown signal intercepted; draining remaining batches...")
			flush()
			logger.Info("consumer exited cleanly", slog.Int64("final_persisted_count", totalPersisted))
			return

		case <-flushTicker.C:
			flush()

		default:
			// Fetch next message with micro-timeout to remain responsive to ctx.Done and timer
			fetchCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
			msg, err := reader.FetchMessage(fetchCtx)
			cancel()

			if err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				logger.Warn("kafka fetch message warning", slog.String("error", err.Error()))
				continue
			}

			req := &eventv1.IngestRequest{}
			if err := proto.Unmarshal(msg.Value, req); err != nil {
				logger.Warn("failed to decode protobuf event payload; skipping message",
					slog.String("key", string(msg.Key)),
					slog.String("error", err.Error()),
				)
				continue
			}

			eventsBatch = append(eventsBatch, req)
			messagesBatch = append(messagesBatch, msg)

			if len(eventsBatch) >= batchSize {
				flush()
			}
		}
	}
}
