package broker

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer defines the interface for publishing events to a message broker.
type Producer interface {
	// Publish writes a single message to the specified topic with a key and value payload.
	Publish(ctx context.Context, topic string, key string, value []byte) error
	// PublishBatch writes a slice of messages in a single batched operation.
	PublishBatch(ctx context.Context, msgs []kafka.Message) error
	// AsyncErrors returns the count of failed async batch publish operations.
	AsyncErrors() uint64
	// DLQCount returns the count of messages safely saved to the Dead Letter Queue.
	DLQCount() uint64
	// Close safely flushes any buffered messages and closes the producer.
	Close() error
}

// Config holds configuration parameters for the Kafka producer.
type Config struct {
	// Brokers is a list of Kafka broker addresses (e.g. ["localhost:9092"]).
	Brokers []string
	// DefaultTopic is the default topic to write to if none is provided in Publish.
	DefaultTopic string
	// BatchSize is the maximum number of messages to buffer before flushing.
	BatchSize int
	// BatchTimeout is the maximum duration to buffer messages before flushing.
	BatchTimeout time.Duration
	// Async enables asynchronous writes for maximum gRPC ingestion throughput.
	Async bool
	// DLQPath is the optional file path for writing messages that fail async delivery.
	DLQPath string
}

type kafkaProducer struct {
	writer       *kafka.Writer
	defaultTopic string
	asyncErrors  atomic.Uint64
	dlqCount     atomic.Uint64
	dlqFile      *os.File
	dlqMu        sync.Mutex
}

// NewKafkaProducer creates a new high-throughput batched Kafka writer with DLQ fallback.
func NewKafkaProducer(cfg Config) Producer {
	if len(cfg.Brokers) == 0 {
		cfg.Brokers = []string{"127.0.0.1:9092"}
	}
	if cfg.DefaultTopic == "" {
		cfg.DefaultTopic = "events"
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.BatchTimeout <= 0 {
		cfg.BatchTimeout = 5 * time.Millisecond
	}

	kp := &kafkaProducer{
		defaultTopic: cfg.DefaultTopic,
	}

	if cfg.DLQPath != "" {
		if f, err := os.OpenFile(cfg.DLQPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err == nil {
			kp.dlqFile = f
		}
	}

	w := &kafka.Writer{
		Addr:                   kafka.TCP(cfg.Brokers...),
		Balancer:               &kafka.LeastBytes{},
		BatchSize:              cfg.BatchSize,
		BatchTimeout:           cfg.BatchTimeout,
		WriteBackoffMin:        10 * time.Millisecond,
		Async:                  cfg.Async,
		Compression:            kafka.Snappy,
		AllowAutoTopicCreation: true,
	}

	if cfg.Async {
		w.Completion = func(msgs []kafka.Message, err error) {
			if err != nil {
				count := uint64(len(msgs))
				kp.asyncErrors.Add(count)

				if kp.dlqFile != nil {
					kp.dlqMu.Lock()
					nowStr := time.Now().Format(time.RFC3339Nano)
					for _, m := range msgs {
						b64 := base64.StdEncoding.EncodeToString(m.Value)
						fmt.Fprintf(kp.dlqFile, "%s\t%s\t%s\t%s\t%s\n", nowStr, m.Topic, string(m.Key), err.Error(), b64)
					}
					kp.dlqCount.Add(count)
					kp.dlqMu.Unlock()
				}
			}
		}
	}

	kp.writer = w
	return kp
}

// Publish sends a message to the broker.
func (p *kafkaProducer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	if topic == "" {
		topic = p.defaultTopic
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: value,
		Time:  time.Now(),
	}
	return p.writer.WriteMessages(ctx, msg)
}

// PublishBatch writes multiple messages directly to Kafka.
func (p *kafkaProducer) PublishBatch(ctx context.Context, msgs []kafka.Message) error {
	for i := range msgs {
		if msgs[i].Topic == "" {
			msgs[i].Topic = p.defaultTopic
		}
	}
	return p.writer.WriteMessages(ctx, msgs...)
}

// AsyncErrors returns the total count of failed asynchronous message deliveries.
func (p *kafkaProducer) AsyncErrors() uint64 {
	return p.asyncErrors.Load()
}

// DLQCount returns the total count of messages persisted to the dead letter queue.
func (p *kafkaProducer) DLQCount() uint64 {
	return p.dlqCount.Load()
}

// Close flushes buffered messages and closes connections.
func (p *kafkaProducer) Close() error {
	p.dlqMu.Lock()
	if p.dlqFile != nil {
		_ = p.dlqFile.Close()
		p.dlqFile = nil
	}
	p.dlqMu.Unlock()
	return p.writer.Close()
}
