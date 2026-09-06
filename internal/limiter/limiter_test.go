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
