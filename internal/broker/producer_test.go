package broker

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestKafkaProducer_Publish(t *testing.T) {
	cfg := Config{
		Brokers:      []string{"127.0.0.1:9092"},
		DefaultTopic: "events",
		BatchSize:    1,
		BatchTimeout: 10 * time.Millisecond,
		Async:        false,
	}

	producer := NewKafkaProducer(cfg)
	defer producer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	key := fmt.Sprintf("test-key-%d", time.Now().UnixNano())
	value := []byte(`{"event_id":"test-1","status":"ok"}`)

	err := producer.Publish(ctx, "events", key, value)
	if err != nil {
		t.Fatalf("failed to publish message to kafka: %v", err)
	}
}
