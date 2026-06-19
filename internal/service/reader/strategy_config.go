package reader

import (
	"context"
	"fmt"
	"strings"

	"gidh-backend/internal/service/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StrategyConfigReader struct {
	pool *pgxpool.Pool
}

func NewStrategyConfigReader(pool *pgxpool.Pool) *StrategyConfigReader {
	return &StrategyConfigReader{pool: pool}
}

// FetchLatestConfigs extracts the most recent distinct configuration matrix
// for each stock name across any timeframe configuration present.
func (scr *StrategyConfigReader) FetchLatestConfigs(ctx context.Context) (map[string]*models.OptimizedStrategyConfig, error) {
	configsMap := make(map[string]*models.OptimizedStrategyConfig)

	// DISTINCT ON (stock_name) paired with optimization_date DESC guarantees we grab
	// the absolute newest optimized rule profile for each asset ticker.
	query := `
		SELECT DISTINCT ON (stock_name)
			optimization_date, stock_name, entry_tf, min_volume_rank, min_price_rank, 
			min_tick_rank, eff_scalp_threshold, direction_mode, min_efficiency_slope, 
			long_time_above_vwap_pct, short_time_above_vwap_pct, take_profit_points, 
			stop_loss_points, profit_pain_ratio, signal_count
		FROM optimized_strategy_configs
		ORDER BY stock_name, optimization_date DESC;`

	rows, err := scr.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query optimized strategy configs: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var c models.OptimizedStrategyConfig
		err := rows.Scan(
			&c.OptimizationDate, &c.StockName, &c.EntryTF, &c.MinVolumeRank, &c.MinPriceRank,
			&c.MinTickRank, &c.EffScalpThreshold, &c.DirectionMode, &c.MinEfficiencySlope,
			&c.LongTimeAboveVwapPct, &c.ShortTimeAboveVwapPct, &c.TakeProfitPoints,
			&c.StopLossPoints, &c.ProfitPainRatio, &c.SignalCount,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan optimized strategy config row: %w", err)
		}

		key := strings.ToUpper(c.StockName)
		configsMap[key] = &c
	}

	return configsMap, nil
}
