package reader

import (
	"context"
	"gidh-backend/internal/service/models"

	"github.com/jackc/pgx/v5/pgxpool"
)

type InstrumentReader struct {
	pool *pgxpool.Pool
}

func NewInstrumentReader(pool *pgxpool.Pool) *InstrumentReader {
	return &InstrumentReader{pool: pool}
}

// UpdateBacktestSelection updates the backtest flag.
func (r *InstrumentReader) UpdateBacktestSelection(ctx context.Context, stockNames []string) error {
	// 1. Reset all backtest flags
	_, err := r.pool.Exec(ctx, "UPDATE instrument_configs SET is_backtest = FALSE")
	if err != nil {
		return err
	}

	// 2. Set flag for selected stocks
	_, err = r.pool.Exec(ctx, "UPDATE instrument_configs SET is_backtest = TRUE WHERE stock_name = ANY($1)", stockNames)
	return err
}

// FetchActiveConfigs retrieves all instruments
// Note: Removed 'WHERE is_active = TRUE' since 'is_active' is no longer in the schema.
func (r *InstrumentReader) FetchActiveConfigs(ctx context.Context) ([]models.InstrumentConfig, error) {
	rows, err := r.pool.Query(ctx, `
        SELECT instrument_token, stock_name, is_backtest
        FROM instrument_configs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []models.InstrumentConfig
	for rows.Next() {
		var c models.InstrumentConfig
		if err := rows.Scan(
			&c.Token, &c.Name, &c.IsBacktest,
		); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}

	return configs, nil
}

// FetchBacktestConfigs retrieves only instruments marked for backtesting
func (r *InstrumentReader) FetchBacktestConfigs(ctx context.Context) ([]models.InstrumentConfig, error) {
	rows, err := r.pool.Query(ctx, `
        SELECT instrument_token, stock_name, is_backtest
        FROM instrument_configs 
        WHERE is_backtest = TRUE`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []models.InstrumentConfig
	for rows.Next() {
		var c models.InstrumentConfig
		if err := rows.Scan(
			&c.Token, &c.Name, &c.IsBacktest,
		); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}

	return configs, nil
}

func (r *InstrumentReader) FetchConfigsByStockNames(ctx context.Context, stockNames []string) ([]models.InstrumentConfig, error) {
	rows, err := r.pool.Query(ctx, `
        SELECT instrument_token, stock_name, is_backtest
        FROM instrument_configs 
        WHERE stock_name = ANY($1)`, stockNames)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var configs []models.InstrumentConfig
	for rows.Next() {
		var c models.InstrumentConfig
		if err := rows.Scan(
			&c.Token, &c.Name, &c.IsBacktest,
		); err != nil {
			return nil, err
		}
		configs = append(configs, c)
	}
	return configs, nil
}

// FetchADVProfiles retrieves the 30-day average daily volume for all instruments.
func (r *InstrumentReader) FetchADVProfiles(ctx context.Context) (map[uint32]float64, error) {
	advMap := make(map[uint32]float64)

	// Querying bigint adv_30d from public.instrument_profile[cite: 3]
	query := `SELECT instrument_token, adv_30d FROM public.instrument_profile`

	rows, err := r.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var token uint32
		var adv int64 // DB type is bigint[cite: 3]
		if err := rows.Scan(&token, &adv); err != nil {
			return nil, err
		}
		advMap[token] = float64(adv) // Pipeline expects float64
	}
	return advMap, nil
}
