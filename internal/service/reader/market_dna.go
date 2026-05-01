package reader

import (
	"context"
	"encoding/json"
	"fmt"
	"gidh-backend/internal/service/models"
	"github.com/jackc/pgx/v5/pgxpool"
	"time"
)

type DNAReader struct {
	pool *pgxpool.Pool
}

func NewDNAReader(pool *pgxpool.Pool) *DNAReader {
	return &DNAReader{pool: pool}
}

// FetchMarketDNA loads the latest DNA for active instruments into a RAM Map.
// internal/service/reader/market_dna.go

// FetchMarketDNA loads the DNA for a specific trading date into a RAM Map.
func (r *DNAReader) FetchMarketDNA(ctx context.Context, targetDate time.Time) (map[uint32]*models.MarketDNA, error) {
	dnaMap := make(map[uint32]*models.MarketDNA)

	// Filter by targetDate to ensure we load the correct baseline for the session
	rows, err := r.pool.Query(ctx, `
		SELECT instrument_token, stock_name, trading_date, poc_5d, vah_5d, val_5d, 
		       macro_hvns, macro_lvns, time_buckets
		FROM gidh_market_dna
		WHERE trading_date = $1::date
	`, targetDate)
	if err != nil {
		return nil, fmt.Errorf("failed to query market_dna: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var dna models.MarketDNA
		var hvnsJSON, lvnsJSON, bucketsJSON []byte

		err := rows.Scan(
			&dna.InstrumentToken, &dna.StockName, &dna.TradingDate,
			&dna.POC, &dna.VAH, &dna.VAL,
			&hvnsJSON, &lvnsJSON, &bucketsJSON, // Added lvnsJSON
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan dna row: %w", err)
		}

		// Unmarshal JSONB columns
		if err := json.Unmarshal(hvnsJSON, &dna.MacroHVNs); err != nil {
			return nil, fmt.Errorf("failed to unmarshal macro_hvns for %s: %w", dna.StockName, err)
		}
		if err := json.Unmarshal(lvnsJSON, &dna.MacroLVNs); err != nil { // Added unmarshal for LVNs
			return nil, fmt.Errorf("failed to unmarshal macro_lvns for %s: %w", dna.StockName, err)
		}
		if err := json.Unmarshal(bucketsJSON, &dna.TimeBuckets); err != nil {
			return nil, fmt.Errorf("failed to unmarshal time_buckets for %s: %w", dna.StockName, err)
		}

		dnaMap[dna.InstrumentToken] = &dna
	}

	return dnaMap, nil
}
