// Package limiter provides distributed per-tenant rate limiting using a
// 2-tier token lease architecture. Local in-memory atomic counters serve the
// fast path (~5ns), while a Redis-backed Lua script handles batch token lease
// renewal via singleflight coordination (sync.Cond). Idle tenant buckets are
// automatically evicted after 5 minutes of inactivity.
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
	// AllowN checks if n requests from the given tenant are allowed under the rate limit.
	AllowN(ctx context.Context, tenantID string, n int64) (bool, error)
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
	tokens       atomic.Int64
	lastAccessed atomic.Int64
	renewing     atomic.Bool
	mu           sync.Mutex
	cond         *sync.Cond
	cachedKey    string
	cachedSec    int64
}

func newTenantBucket() *tenantBucket {
	b := &tenantBucket{}
	b.cond = sync.NewCond(&b.mu)
	b.lastAccessed.Store(time.Now().Unix())
	return b
}

type tieredLimiter struct {
	client       *redis.Client
	defaultLimit int64
	windowSec    int64
	batchSize    int64
	buckets      sync.Map // tenantID -> *tenantBucket
	stopEvict    chan struct{}
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

	lim := &tieredLimiter{
		client:       rdb,
		defaultLimit: cfg.DefaultLimit,
		windowSec:    windowSec,
		batchSize:    cfg.LeaseBatchSize,
		stopEvict:    make(chan struct{}),
	}
	go lim.evictionLoop()
	return lim, nil
}

func (l *tieredLimiter) evictionLoop() {
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			l.buckets.Range(func(key, val any) bool {
				b := val.(*tenantBucket)
				if now-b.lastAccessed.Load() > 300 {
					l.buckets.Delete(key)
				}
				return true
			})
		case <-l.stopEvict:
			return
		}
	}
}

func (l *tieredLimiter) getBucket(tenantID string) *tenantBucket {
	val, ok := l.buckets.Load(tenantID)
	if ok {
		b := val.(*tenantBucket)
		b.lastAccessed.Store(time.Now().Unix())
		return b
	}
	b := newTenantBucket()
	actual, _ := l.buckets.LoadOrStore(tenantID, b)
	res := actual.(*tenantBucket)
	res.lastAccessed.Store(time.Now().Unix())
	return res
}

// Allow checks if a single request from the given tenant is allowed.
func (l *tieredLimiter) Allow(ctx context.Context, tenantID string) (bool, error) {
	return l.AllowN(ctx, tenantID, 1)
}

// AllowN executes the fast-path local atomic check for n tokens, leasing from Redis only when local quota is depleted.
func (l *tieredLimiter) AllowN(ctx context.Context, tenantID string, n int64) (bool, error) {
	if n <= 0 {
		return true, nil
	}
	if tenantID == "" {
		tenantID = "default"
	}

	b := l.getBucket(tenantID)

	// Fast path: lock-free atomic decrement
	if remaining := b.tokens.Add(-n); remaining >= 0 {
		return true, nil
	}

	// Singleflight coordination: exactly one goroutine executes the Redis lease renewal
	if b.renewing.CompareAndSwap(false, true) {
		defer func() {
			b.mu.Lock()
			b.renewing.Store(false)
			b.cond.Broadcast()
			b.mu.Unlock()
		}()

		requestBatch := l.batchSize
		if n > requestBatch {
			requestBatch = n
		}

		nowSec := time.Now().Unix() / l.windowSec
		if b.cachedSec != nowSec || b.cachedKey == "" {
			b.cachedSec = nowSec
			b.cachedKey = fmt.Sprintf("ratelimit:%s:%d", tenantID, nowSec)
		}
		key := b.cachedKey
		ttl := l.windowSec * 2

		granted, err := tokenLeaseScript.Run(ctx, l.client, []string{key}, ttl, l.defaultLimit, requestBatch).Int64()
		if err != nil {
			return false, fmt.Errorf("redis lease error: %w", err)
		}

		if granted < n {
			// Quota exhausted or insufficient for this window
			b.tokens.Store(0)
			return false, nil
		}

		// Consume n tokens for this batch, store remaining
		b.tokens.Store(granted - n)
		return true, nil
	}

	// Waiter path: wait for the designated renewer to broadcast completion
	b.mu.Lock()
	for b.renewing.Load() {
		b.cond.Wait()
	}
	b.mu.Unlock()

	// After renewal, retry fast-path atomic deduction
	if remaining := b.tokens.Add(-n); remaining >= 0 {
		return true, nil
	}

	return false, nil
}

func (l *tieredLimiter) Close() error {
	close(l.stopEvict)
	return l.client.Close()
}
