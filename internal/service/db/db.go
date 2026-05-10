package db

import (
	"context"
	"fmt"
	"time"

	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

var activePool *pgxpool.Pool

// InitDB initializes the database connection pool based on the provided URL
func InitDB(ctx context.Context, dbURL string) error {
	var err error
	activePool, err = pgxpool.New(ctx, dbURL)
	if err != nil {
		return err
	}

	// Test connection
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := activePool.Ping(timeoutCtx); err != nil {
		return err
	}

	logger.Info("Database connection established")
	return nil
}

// GetPool returns the currently active database connection pool
func GetPool() *pgxpool.Pool {
	return activePool
}

// CloseDB closes the active database connection pool
func CloseDB() {
	if activePool != nil {
		activePool.Close()
		logger.Info("Database connection closed")
	}
}

// CleanupBacktestData removes records for a specific date from the core tables.
func CleanupBacktestData(ctx context.Context, dateStr string) error {
	if activePool == nil {
		return fmt.Errorf("database pool not initialized")
	}

	logger.Infof("Cleaning up existing backtest data for date: %s", dateStr)

	// Using a transaction to ensure atomic cleanup
	tx, err := activePool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// List of tables and their respective time/date columns
	queries := []struct {
		table string
		col   string
	}{
		{"live_ticks", "timestamp"},
		{"live_order_depth", "timestamp"},
		{"gidh_bars", "timestamp"},
		{"gidh_volume_profiles", "trading_date"},
	}

	for _, q := range queries {
		// Cast TIMESTAMPTZ to date for comparison
		query := fmt.Sprintf("DELETE FROM %s WHERE %s::date = $1", q.table, q.col)
		if _, err := tx.Exec(ctx, query, dateStr); err != nil {
			return fmt.Errorf("failed to cleanup table %s: %w", q.table, err)
		}
	}

	// Special case for volume profiles which typically uses a DATE type column
	if _, err := tx.Exec(ctx, "DELETE FROM gidh_volume_profiles WHERE trading_date = $1", dateStr); err != nil {
		return fmt.Errorf("failed to cleanup gidh_volume_profiles: %w", err)
	}

	return tx.Commit(ctx)
}
