package db

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/models"
	"strings"
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

// LoadSessionSnapshotFromDB extracts the current day's saved rows from core hypertables
// to survive an engine application restart and re-populate live RAM state caches.
func LoadSessionSnapshotFromDB(ctx context.Context, pool *pgxpool.Pool) ([]models.OrderBookEntry, []models.Position, error) {
	if pool == nil {
		return nil, nil, fmt.Errorf("database pool is not initialized")
	}

	// Target the active current date string sequence for lookups
	currentDate := time.Now().Format("2006-01-02")
	logger.Infof("Loading database session snapshot records for date: %s", currentDate)

	// 1. Recover Order Ledger Snapshot
	orders := make([]models.OrderBookEntry, 0)
	orderRows, err := pool.Query(ctx, `
	SELECT order_id, symbol, side, order_type, quantity, filled_qty, price, status, timestamp, user_email
	FROM gidh_orders
	WHERE trading_date::date = $1::date`, currentDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to recover session orders from gidh_orders: %w", err)
	}
	defer orderRows.Close()

	for orderRows.Next() {
		var o models.OrderBookEntry
		var side, oType string
		err := orderRows.Scan(
			&o.OrderID, &o.Symbol, &side, &oType, &o.Qty,
			&o.FilledQty, &o.Price, &o.Status, &o.Timestamp, &o.UserEmail,
		)
		if err != nil {
			logger.Errorf("Failed to scan recovered order row snapshot: %v", err)
			continue
		}
		o.Symbol = strings.ToUpper(o.Symbol)
		o.Side = strings.ToUpper(side)
		o.OrderType = strings.ToUpper(oType)
		o.Status = strings.ToUpper(o.Status)
		orders = append(orders, o)
	}

	// 2. Recover Positions Snapshot (Including local RAM target_price/stop_loss_price coordinates)
	positions := make([]models.Position, 0)
	posRows, err := pool.Query(ctx, `
		SELECT symbol, product, side, net_quantity, avg_price, realized_pnl, target_price, stop_loss_price
		FROM gidh_positions
		WHERE trading_date = $1::date`, currentDate)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to recover session positions from gidh_positions: %w", err)
	}
	defer posRows.Close()

	for posRows.Next() {
		var p models.Position
		var side string
		err := posRows.Scan(
			&p.Symbol, &p.Product, &side, &p.NetQuantity,
			&p.AveragePrice, &p.RealizedPnL, &p.TargetPrice, &p.StopLossPrice,
		)
		if err != nil {
			logger.Errorf("Failed to scan recovered position row snapshot: %v", err)
			continue
		}
		p.Symbol = strings.ToUpper(p.Symbol)
		p.Product = strings.ToUpper(p.Product)
		p.Side = strings.ToUpper(side)
		positions = append(positions, p)
	}

	return orders, positions, nil
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
