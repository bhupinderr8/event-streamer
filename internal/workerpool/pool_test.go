package workerpool

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"github.com/segmentio/kafka-go"
)

type mockProducer struct {
	publishedCount atomic.Int64
}

func (m *mockProducer) Publish(ctx context.Context, topic string, key string, value []byte) error {
	m.publishedCount.Add(1)
	return nil
}

func (m *mockProducer) PublishBatch(ctx context.Context, msgs []kafka.Message) error {
	m.publishedCount.Add(int64(len(msgs)))
	return nil
}

func (m *mockProducer) AsyncErrors() uint64 {
	return 0
}

func (m *mockProducer) DLQCount() uint64 {
	return 0
}

func (m *mockProducer) Close() error {
	return nil
}

func TestWorkerPool_BatchingAndStop(t *testing.T) {
	mock := &mockProducer{}
	pool := New(mock, 4, 1000)

	numEvents := 500
	for i := 0; i < numEvents; i++ {
		req := &eventv1.IngestRequest{
			EventId:   fmt.Sprintf("evt-%d", i),
			TenantId:  "test-tenant",
			Payload:   "data",
			Timestamp: time.Now().UnixNano(),
		}
		if !pool.Submit(req) {
			t.Fatalf("failed to submit event %d", i)
		}
	}

	pool.Stop()

	if published := mock.publishedCount.Load(); published != int64(numEvents) {
		t.Fatalf("expected %d events published, got %d", numEvents, published)
	}

	if pool.PublishErrors() != 0 {
		t.Fatalf("expected 0 publish errors, got %d", pool.PublishErrors())
	}
}

func TestWorkerPool_SubmitBatch(t *testing.T) {
	mock := &mockProducer{}
	pool := New(mock, 4, 1000)

	batchSize := 200
	events := make([]*eventv1.IngestRequest, batchSize)
	for i := 0; i < batchSize; i++ {
		events[i] = &eventv1.IngestRequest{
			EventId:   fmt.Sprintf("evt-batch-%d", i),
			TenantId:  "test-tenant",
			Payload:   "data",
			Timestamp: time.Now().UnixNano(),
		}
	}

	accepted, ok := pool.SubmitBatch(events)
	if !ok || accepted != batchSize {
		t.Fatalf("expected SubmitBatch to accept %d events, got %d (ok: %v)", batchSize, accepted, ok)
	}

	pool.Stop()

	if published := mock.publishedCount.Load(); published != int64(batchSize) {
		t.Fatalf("expected %d events published, got %d", batchSize, published)
	}
}

func TestWorkerPool_SubmitAfterStop(t *testing.T) {
	mock := &mockProducer{}
	pool := New(mock, 2, 1024)
	pool.Stop()

	req := &eventv1.IngestRequest{
		EventId:   "evt-after-stop",
		TenantId:  "test-tenant",
		Payload:   "data",
		Timestamp: time.Now().UnixNano(),
	}
	if pool.Submit(req) {
		t.Fatal("expected Submit to return false after Stop, but got true")
	}

	accepted, ok := pool.SubmitBatch([]*eventv1.IngestRequest{req})
	if ok || accepted != 0 {
		t.Fatal("expected SubmitBatch to return 0, false after Stop")
	}
}

func TestWorkerPool_QueueDepth(t *testing.T) {
	mock := &mockProducer{}
	pool := New(mock, 4, 1024)
	defer pool.Stop()

	if depth := pool.QueueDepth(); depth != 0 {
		t.Fatalf("expected queue depth 0, got %d", depth)
	}
}

