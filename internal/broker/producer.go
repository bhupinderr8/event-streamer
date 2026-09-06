package broker

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

// Producer defines the interface for publishing events to a message broker.
type Producer interface {
	// Publish writes a message to the specified topic with a key and value payload.
	Publish(ctx context.Context, topic string, key string, value []byte) error
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
}

type kafkaProducer struct {
	writer       *kafka.Writer
	defaultTopic string
}

// NewKafkaProducer creates a new high-throughput batched Kafka writer.
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

	return &kafkaProducer{
		writer:       w,
		defaultTopic: cfg.DefaultTopic,
	}
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

// Close flushes buffered messages and closes connections.
func (p *kafkaProducer) Close() error {
	return p.writer.Close()
}
