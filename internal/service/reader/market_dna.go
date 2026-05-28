package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"gidh-backend/internal/service/models"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DNAReader struct {
	pool *pgxpool.Pool
}

func NewDNAReader(pool *pgxpool.Pool) *DNAReader {
	return &DNAReader{pool: pool}
}

// FetchMarketDNA loads the DNA for a specific trading date into a RAM Map.
func (r *DNAReader) FetchMarketDNA(ctx context.Context, targetDate time.Time) (map[uint32]*models.MarketDNA, error) {
	dnaMap := make(map[uint32]*models.MarketDNA)

	// Filter by targetDate to ensure we load the correct baseline for the session
	rows, err := r.pool.Query(ctx, `
		SELECT instrument_token, stock_name, trading_date, poc_5d, vah_5d, val_5d, 
		       macro_hvns, macro_lvns, time_buckets, interval_percentiles
		FROM gidh_market_dna
		WHERE trading_date = $1::date
	`, targetDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query market_dna: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dna models.MarketDNA
		var hvnsJSON, lvnsJSON, bucketsJSON, percentilesJSON []byte

		err := rows.Scan(
			&dna.InstrumentToken, &dna.StockName, &dna.TradingDate,
			&dna.POC, &dna.VAH, &dna.VAL,
			&hvnsJSON, &lvnsJSON, &bucketsJSON, &percentilesJSON,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dna row: %w", err)
		}

		// Unmarshal JSONB columns
		if err := json.Unmarshal(hvnsJSON, &dna.MacroHVNs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal macro_hvns for %s: %w", dna.StockName, err)
		}
		if err := json.Unmarshal(lvnsJSON, &dna.MacroLVNs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal macro_lvns for %s: %w", dna.StockName, err)
		}
		if err := json.Unmarshal(bucketsJSON, &dna.TimeBuckets); err != nil {
			return nil, fmt.Errorf("failed to unmarshal time_buckets for %s: %w", dna.StockName, err)
		}
		// 🔥 Unmarshal newly extracted multi-timeframe baseline thresholds
		if len(percentilesJSON) > 0 && string(percentilesJSON) != "{}" {
			if err := json.Unmarshal(percentilesJSON, &dna.IntervalPercentiles); err != nil {
				return nil, fmt.Errorf("failed to unmarshal interval_percentiles for %s: %w", dna.StockName, err)
			}
		} else {
			dna.IntervalPercentiles = make(map[string]models.PercentileThresholds)
		}

		dnaMap[dna.InstrumentToken] = &dna
	}

	return dnaMap, nil
}
