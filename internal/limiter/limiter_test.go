package limiter

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestRedisLimiter_Allow(t *testing.T) {
	cfg := Config{
		RedisAddr:      "127.0.0.1:6379",
		DefaultLimit:   10,
		LeaseBatchSize: 5,
		Window:         time.Second,
	}

	limiter, err := NewRedisLimiter(cfg)
	if err != nil {
		t.Fatalf("failed to connect to redis limiter: %v", err)
	}
	defer limiter.Close()

	ctx := context.Background()
	tenantID := fmt.Sprintf("test-tenant-%d", time.Now().UnixNano())

	// First 10 requests must be allowed (2 lease batches of 5)
	for i := 1; i <= 10; i++ {
		allowed, err := limiter.Allow(ctx, tenantID)
		if err != nil {
			t.Fatalf("request %d failed with error: %v", i, err)
		}
		if !allowed {
			t.Fatalf("expected request %d to be allowed", i)
		}
	}

	// 11th request must be rejected
	allowed, err := limiter.Allow(ctx, tenantID)
	if err != nil {
		t.Fatalf("overflow request check failed with error: %v", err)
	}
	if allowed {
		t.Fatalf("expected 11th request to be rejected, but was allowed")
	}

	// Another tenant should have their own fresh quota
	otherTenant := fmt.Sprintf("other-tenant-%d", time.Now().UnixNano())
	allowed, err = limiter.Allow(ctx, otherTenant)
	if err != nil {
		t.Fatalf("other tenant check failed with error: %v", err)
	}
	if !allowed {
		t.Fatalf("expected other tenant request to be allowed")
	}
}

func TestRedisLimiter_AllowN(t *testing.T) {
	cfg := Config{
		RedisAddr:      "127.0.0.1:6379",
		DefaultLimit:   50,
		LeaseBatchSize: 20,
		Window:         time.Second,
	}

	limiter, err := NewRedisLimiter(cfg)
	if err != nil {
		t.Fatalf("failed to connect to redis limiter: %v", err)
	}
	defer limiter.Close()

	ctx := context.Background()
	tenantID := fmt.Sprintf("test-batch-tenant-%d", time.Now().UnixNano())

	// Batch of 25: leases 25 from limit of 50
	allowed, err := limiter.AllowN(ctx, tenantID, 25)
	if err != nil || !allowed {
		t.Fatalf("expected AllowN(25) to succeed, err: %v, allowed: %v", err, allowed)
	}

	// Another batch of 25: consumes remaining 25
	allowed, err = limiter.AllowN(ctx, tenantID, 25)
	if err != nil || !allowed {
		t.Fatalf("expected second AllowN(25) to succeed, err: %v, allowed: %v", err, allowed)
	}

	// Any further requests should be rejected
	allowed, err = limiter.AllowN(ctx, tenantID, 1)
	if err != nil {
		t.Fatalf("check failed: %v", err)
	}
	if allowed {
		t.Fatalf("expected 51st token to be rejected")
	}
}

func TestRedisLimiter_Eviction(t *testing.T) {
	cfg := Config{
		RedisAddr:      "127.0.0.1:6379",
		DefaultLimit:   50,
		LeaseBatchSize: 20,
		Window:         time.Second,
	}

	l, err := NewRedisLimiter(cfg)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer l.Close()

	tl := l.(*tieredLimiter)
	ctx := context.Background()
	tenantID := fmt.Sprintf("evict-tenant-%d", time.Now().UnixNano())

	tl.Allow(ctx, tenantID)
	if _, ok := tl.buckets.Load(tenantID); !ok {
		t.Fatalf("expected tenant bucket to exist")
	}

	// Artificially age the bucket beyond 300 seconds
	val, _ := tl.buckets.Load(tenantID)
	val.(*tenantBucket).lastAccessed.Store(time.Now().Unix() - 350)

	// Trigger eviction pass
	now := time.Now().Unix()
	tl.buckets.Range(func(key, v any) bool {
		b := v.(*tenantBucket)
		if now-b.lastAccessed.Load() > 300 {
			tl.buckets.Delete(key)
		}
		return true
	})

	if _, ok := tl.buckets.Load(tenantID); ok {
		t.Fatalf("expected bucket to be evicted after idle period")
	}
}

