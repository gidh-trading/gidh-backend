package reader

import (
	"context"
	"gidh-backend/internal/service/models"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type InstrumentReader struct {
	pool *pgxpool.Pool
}

// PricePotentialRecord holds a single row representing an asset profile trajectory
type PricePotentialRecord struct {
	StockName string
	Timeframe string
	P75       float64
	P90       float64
}

func NewInstrumentReader(pool *pgxpool.Pool) *InstrumentReader {
	return &InstrumentReader{pool: pool}
}

// UpdateBacktestSelection updates the backtest flag.
func (ir *InstrumentReader) UpdateBacktestSelection(ctx context.Context, stockNames []string) error {
	// 1. Reset all backtest flags
	_, err := ir.pool.Exec(ctx, "UPDATE instrument_configs SET is_backtest = FALSE")
	if err != nil {
		return err
	}

	// 2. Set flag for selected stocks
	_, err = ir.pool.Exec(ctx, "UPDATE instrument_configs SET is_backtest = TRUE WHERE stock_name = ANY($1)", stockNames)
	return err
}

// FetchActiveConfigs retrieves all instruments
// Note: Removed 'WHERE is_active = TRUE' since 'is_active' is no longer in the schema.
func (ir *InstrumentReader) FetchActiveConfigs(ctx context.Context) ([]models.InstrumentConfig, error) {
	rows, err := ir.pool.Query(ctx, `
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
func (ir *InstrumentReader) FetchBacktestConfigs(ctx context.Context) ([]models.InstrumentConfig, error) {
	rows, err := ir.pool.Query(ctx, `
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

func (ir *InstrumentReader) FetchConfigsByStockNames(ctx context.Context, stockNames []string) ([]models.InstrumentConfig, error) {
	rows, err := ir.pool.Query(ctx, `
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

// FetchInstrumentProfiles retrieves complete parameter maps directly from the profile properties data table
// for a specific trading session date.
func (ir *InstrumentReader) FetchInstrumentProfiles(ctx context.Context, targetDate time.Time) (map[uint32]*models.InstrumentProfile, error) {
	profilesMap := make(map[uint32]*models.InstrumentProfile)

	// Join on the static instrument_token, and filter by the daily snapshot date from instrument_profile
	query := `
       SELECT ic.stock_name, ip.instrument_token, ip.bucket_size, ip.atr_14, ip.adr_pct, ip.adv_30d, ip.adv_val_30d 
       FROM instrument_profile ip
       INNER JOIN instrument_configs ic ON ip.instrument_token = ic.instrument_token
       WHERE ip.trading_date = $1::date`

	rows, err := ir.pool.Query(ctx, query, targetDate)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var p models.InstrumentProfile
		if err := rows.Scan(
			&p.StockName,
			&p.InstrumentToken,
			&p.BucketSize,
			&p.ATR14,
			&p.ADRPct,
			&p.ADV30d,
			&p.ADVVal30d,
		); err != nil {
			return nil, err
		}
		profilesMap[p.InstrumentToken] = &p
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return profilesMap, nil
}

// FetchAllPricePotentials reads the full mathematical percentiles from the database table.
func (ir *InstrumentReader) FetchAllPricePotentials(ctx context.Context) (models.TargetMatrix, error) {
	matrix := make(models.TargetMatrix)

	query := `
       SELECT stock_name, timeframe, p75, p90 
       FROM public.gidh_bars_price_potential`

	rows, err := ir.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var stockName string
		var timeframe string
		var p75, p90 float64

		if err := rows.Scan(&stockName, &timeframe, &p75, &p90); err != nil {
			return nil, err
		}

		// Allocate nested map structure dynamically on the fly
		if matrix[stockName] == nil {
			matrix[stockName] = make(map[string]models.PricePotential)
		}

		matrix[stockName][timeframe] = models.PricePotential{
			P75: p75,
			P90: p90,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return matrix, nil
}
