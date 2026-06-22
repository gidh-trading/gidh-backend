package reader

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/models"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type VWAPPercentileReader struct {
	pool *pgxpool.Pool
}

func NewVWAPPercentileReader(pool *pgxpool.Pool) *VWAPPercentileReader {
	return &VWAPPercentileReader{pool: pool}
}

// FetchVWAPDistancePercentiles loads the VWAP extension thresholds for a specific trading date into a RAM Map.
func (r *VWAPPercentileReader) FetchVWAPDistancePercentiles(ctx context.Context, targetDate time.Time) (map[uint32]*models.VWAPDistancePercentile, error) {
	vpMap := make(map[uint32]*models.VWAPDistancePercentile)

	query := `
		SELECT instrument_token, stock_name, trading_date, 
		       pos_p50, pos_p75, pos_p90, pos_p97, pos_p99,
		       neg_p50, neg_p75, neg_p90, neg_p97, neg_p99
		FROM gidh_vwap_distance_percentiles
		WHERE trading_date = $1::date
	`

	rows, err := r.pool.Query(ctx, query, targetDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query vwap_distance_percentiles: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var vp models.VWAPDistancePercentile

		err := rows.Scan(
			&vp.InstrumentToken, &vp.StockName, &vp.TradingDate,
			&vp.PosP50, &vp.PosP75, &vp.PosP90, &vp.PosP97, &vp.PosP99,
			&vp.NegP50, &vp.NegP75, &vp.NegP90, &vp.NegP97, &vp.NegP99,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan vwap percentile row: %w", err)
		}

		vpMap[vp.InstrumentToken] = &vp
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error during vwap percentile row iteration: %w", err)
	}

	return vpMap, nil
}
