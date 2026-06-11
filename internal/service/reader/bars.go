package reader

import (
	"context"
	"fmt"
	"time"

	"gidh-backend/internal/service/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type BarReader struct {
	pool *pgxpool.Pool
}

func NewBarReader(pool *pgxpool.Pool) *BarReader {
	return &BarReader{pool: pool}
}

// FetchSessionBars loads all closed bars for the current trading day in chronological order
func (r *BarReader) FetchSessionBars(ctx context.Context, targetDate time.Time, symbols []string) ([]*models.Bar, error) {
	// Truncate times to extract the absolute boundary of the trading day
	startOfDay := time.Date(targetDate.Year(), targetDate.Month(), targetDate.Day(), 0, 0, 0, 0, targetDate.Location())
	endOfDay := startOfDay.Add(24 * time.Hour)

	// We select 'analytics' as a single jsonb object column matching your schema configuration
	rows, err := r.pool.Query(ctx, `
		SELECT timestamp, instrument_token, stock_name, timeframe, 
		       open, high, low, close, volume, tick_count, vwap, 
		       poc, vah, val, total_buy_qty, total_sell_qty, change_pct, 
		       analytics
		FROM gidh_bars
		WHERE timestamp >= $1 AND timestamp < $2 AND stock_name = ANY($3)
		ORDER BY timestamp ASC
	`, startOfDay, endOfDay, symbols)
	if err != nil {
		return nil, fmt.Errorf("failed to query closed bars: %w", err)
	}
	defer rows.Close()

	var bars []*models.Bar
	for rows.Next() {
		var b models.Bar

		err := rows.Scan(
			&b.Timestamp, &b.InstrumentToken, &b.StockName, &b.Timeframe,
			&b.Open, &b.High, &b.Low, &b.Close, &b.Volume, &b.TickCount, &b.VWAP,
			&b.POC, &b.VAH, &b.VAL, &b.TotalBuyQty, &b.TotalSellQty, &b.ChangePct,
			&b.Analytics,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan bar row with jsonb analytics: %w", err)
		}
		bars = append(bars, &b)
	}

	return bars, nil
}
