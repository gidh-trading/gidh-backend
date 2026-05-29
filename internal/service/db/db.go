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
		return fmt.Errorf("failed to begin cleanup transaction: %w", err)
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
		{"gidh_orders", "trading_date"},
		{"gidh_positions", "trading_date"},
	}

	for _, q := range queries {
		// Use explicit cast mapping to ensure string input aligns perfectly with table types
		query := fmt.Sprintf("DELETE FROM %s WHERE %s::date = $1::date", q.table, q.col)

		logger.Debugf("Executing cleanup query: %s with param: %s", query, dateStr)
		if _, err := tx.Exec(ctx, query, dateStr); err != nil {
			return fmt.Errorf("failed to cleanup table %s: %w", q.table, err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit cleanup transaction: %w", err)
	}

	logger.Infof("Successfully wiped all tables for backtest date: %s", dateStr)
	return nil
}
