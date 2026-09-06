package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	eventv1 "github.com/bhupinder121199/event-streamer/proto"
)

func TestPostgresStorage_InsertBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	store, err := NewPostgresStorage(ctx, "")
	if err != nil {
		t.Fatalf("failed to connect to postgres: %v", err)
	}
	defer store.Close()

	if err := store.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}

	testBatch := []*eventv1.IngestRequest{
		{
			EventId:   fmt.Sprintf("evt-pg-test-1-%d", time.Now().UnixNano()),
			TenantId:  "test-tenant",
			Payload:   `{"action":"login"}`,
			Timestamp: time.Now().UnixNano(),
		},
		{
			EventId:   fmt.Sprintf("evt-pg-test-2-%d", time.Now().UnixNano()),
			TenantId:  "test-tenant",
			Payload:   `{"action":"view"}`,
			Timestamp: time.Now().UnixNano(),
		},
	}

	if err := store.InsertBatch(ctx, testBatch); err != nil {
		t.Fatalf("failed to insert batch: %v", err)
	}

	// Insert duplicate batch to test ON CONFLICT DO NOTHING idempotency
	if err := store.InsertBatch(ctx, testBatch); err != nil {
		t.Fatalf("duplicate batch insert should succeed with DO NOTHING, got: %v", err)
	}
}
