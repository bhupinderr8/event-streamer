package storage

import (
	"context"
	"fmt"
	"time"

	eventv1 "github.com/bhupinder121199/event-streamer/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Storage defines the interface for persisting ingested events to durable relational storage.
type Storage interface {
	// InsertBatch inserts a slice of events into PostgreSQL within a single network batch round-trip.
	InsertBatch(ctx context.Context, events []*eventv1.IngestRequest) error
	// Ping checks the health and readiness of the PostgreSQL connection pool.
	Ping(ctx context.Context) error
	// Close releases all database connections in the pool.
	Close()
}

// PostgresStorage implements Storage using pgx connection pooling and batching.
type PostgresStorage struct {
	pool *pgxpool.Pool
}

// NewPostgresStorage initializes the connection pool and creates the required database tables and indexes.
func NewPostgresStorage(ctx context.Context, connString string) (*PostgresStorage, error) {
	if connString == "" {
		connString = "postgres://streamer:streamer_pass@127.0.0.1:5432/events_db?sslmode=disable"
	}

	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres conn string: %w", err)
	}

	cfg.MaxConns = 32
	cfg.MinConns = 8
	cfg.MaxConnIdleTime = 5 * time.Minute
	cfg.MaxConnLifetime = 1 * time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	store := &PostgresStorage{pool: pool}
	if err := store.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to execute postgres schema migration: %w", err)
	}

	return store, nil
}

func (s *PostgresStorage) migrate(ctx context.Context) error {
	ddl := `
	CREATE TABLE IF NOT EXISTS events (
		event_id VARCHAR(64) PRIMARY KEY,
		tenant_id VARCHAR(64) NOT NULL,
		payload TEXT NOT NULL,
		event_timestamp BIGINT NOT NULL,
		created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
	);
	CREATE INDEX IF NOT EXISTS idx_events_tenant ON events(tenant_id);
	CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(event_timestamp);
	`
	_, err := s.pool.Exec(ctx, ddl)
	return err
}

// InsertBatch pipelines batch inserts using pgx.Batch in a single network round-trip.
// Duplicate event IDs are skipped cleanly via ON CONFLICT (event_id) DO NOTHING.
func (s *PostgresStorage) InsertBatch(ctx context.Context, events []*eventv1.IngestRequest) error {
	if len(events) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	query := `INSERT INTO events (event_id, tenant_id, payload, event_timestamp) VALUES ($1, $2, $3, $4) ON CONFLICT (event_id) DO NOTHING`

	for _, req := range events {
		batch.Queue(query, req.GetEventId(), req.GetTenantId(), req.GetPayload(), req.GetTimestamp())
	}

	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()

	for range events {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("batch insert exec failed: %w", err)
		}
	}

	return br.Close()
}

// Ping checks database availability.
func (s *PostgresStorage) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close closes the pool.
func (s *PostgresStorage) Close() {
	s.pool.Close()
}
