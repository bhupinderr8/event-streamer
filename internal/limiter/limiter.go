package limiter

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Limiter defines the contract for rate limiting tenant requests.
type Limiter interface {
	// Allow checks if a request from the given tenant is allowed under the rate limit.
	Allow(ctx context.Context, tenantID string) (bool, error)
	// Close releases any resources held by the limiter.
	Close() error
}

// Config holds configuration parameters for the Redis rate limiter.
type Config struct {
	// RedisAddr is the host:port of the Redis instance.
	RedisAddr string
	// Password is the optional password for Redis authentication.
	Password string
	// DB is the Redis logical database number.
	DB int
	// DefaultLimit is the maximum allowed requests within the window.
	DefaultLimit int64
	// Window is the duration of the rate limit window.
	Window time.Duration
	// LeaseBatchSize is the number of tokens leased from Redis per round-trip.
	LeaseBatchSize int64
}

// tokenLeaseScript atomically reserves up to `requested` tokens from Redis.
// KEYS[1]: rate limit key for tenant and time window (e.g. ratelimit:{tenant}:{windowSec})
// ARGV[1]: window expiration in seconds
// ARGV[2]: max limit in window
// ARGV[3]: requested batch size
var tokenLeaseScript = redis.NewScript(`
local key = KEYS[1]
local ttl = tonumber(ARGV[1])
local limit = tonumber(ARGV[2])
local requested = tonumber(ARGV[3])

local current = tonumber(redis.call('GET', key) or "0")
if current >= limit then
    return 0
end

local available = limit - current
local granted = math.min(available, requested)
local newCount = redis.call('INCRBY', key, granted)
if newCount == granted then
    redis.call('EXPIRE', key, ttl)
end

return granted
`)

type tenantBucket struct {
	tokens atomic.Int64
	mu     sync.Mutex
}

type tieredLimiter struct {
	client       *redis.Client
	defaultLimit int64
	windowSec    int64
	batchSize    int64
	buckets      sync.Map // tenantID -> *tenantBucket
}

// NewRedisLimiter creates a new high-performance 2-tier token lease rate limiter.
func NewRedisLimiter(cfg Config) (Limiter, error) {
	if cfg.RedisAddr == "" {
		cfg.RedisAddr = "127.0.0.1:6379"
	}
	if cfg.DefaultLimit <= 0 {
		cfg.DefaultLimit = 10000
	}
	if cfg.Window <= 0 {
		cfg.Window = time.Second
	}
	if cfg.LeaseBatchSize <= 0 {
		cfg.LeaseBatchSize = 100 // lease 100 tokens at a time to minimize Redis overhead
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:         cfg.RedisAddr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		PoolSize:     128, // support high concurrency without pool contention
		MinIdleConns: 32,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to redis at %s: %w", cfg.RedisAddr, err)
	}

	windowSec := int64(cfg.Window.Seconds())
	if windowSec <= 0 {
		windowSec = 1
	}

	return &tieredLimiter{
		client:       rdb,
		defaultLimit: cfg.DefaultLimit,
		windowSec:    windowSec,
		batchSize:    cfg.LeaseBatchSize,
	}, nil
}

func (l *tieredLimiter) getBucket(tenantID string) *tenantBucket {
	val, ok := l.buckets.Load(tenantID)
	if ok {
		return val.(*tenantBucket)
	}
	b := &tenantBucket{}
	actual, _ := l.buckets.LoadOrStore(tenantID, b)
	return actual.(*tenantBucket)
}

// Allow executes the fast-path local atomic check, leasing from Redis only when local quota is depleted.
func (l *tieredLimiter) Allow(ctx context.Context, tenantID string) (bool, error) {
	if tenantID == "" {
		tenantID = "default"
	}

	b := l.getBucket(tenantID)

	// Fast path: lock-free atomic decrement
	if remaining := b.tokens.Add(-1); remaining >= 0 {
		return true, nil
	}

	// Slow path: acquire lock to lease the next batch of tokens from Redis
	b.mu.Lock()
	defer b.mu.Unlock()

	// Recheck tokens in case another concurrent goroutine already renewed the lease
	if b.tokens.Load() > 0 {
		b.tokens.Add(-1)
		return true, nil
	}

	// Lease a fresh batch of tokens from Redis
	nowSec := time.Now().Unix() / l.windowSec
	key := fmt.Sprintf("ratelimit:%s:%d", tenantID, nowSec)
	ttl := l.windowSec * 2

	granted, err := tokenLeaseScript.Run(ctx, l.client, []string{key}, ttl, l.defaultLimit, l.batchSize).Int64()
	if err != nil {
		return false, fmt.Errorf("redis lease error: %w", err)
	}

	if granted <= 0 {
		// Quota exhausted for the current window
		b.tokens.Store(0)
		return false, nil
	}

	// Consume 1 token for the current request and store the remaining granted tokens
	b.tokens.Store(granted - 1)
	return true, nil
}

func (l *tieredLimiter) Close() error {
	return l.client.Close()
}
