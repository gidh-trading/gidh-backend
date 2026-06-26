package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DBBacktestSource struct {
	db               *pgxpool.Pool
	dbConnString     string
	date             time.Time
	speedFactor      float64
	instrumentTokens []uint32
	instrumentMap    map[uint32]string

	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.RWMutex
	speedUpdateChan chan struct{}
}

type DBBacktestSourceConfig struct {
	DBConnString     string
	Date             time.Time
	SpeedFactor      float64
	InstrumentTokens []uint32
	InstrumentMap    map[uint32]string
}

func NewDBBacktestSource(cfg *DBBacktestSourceConfig) *DBBacktestSource {
	return &DBBacktestSource{
		dbConnString:     cfg.DBConnString,
		date:             cfg.Date,
		speedFactor:      cfg.SpeedFactor,
		instrumentTokens: cfg.InstrumentTokens,
		instrumentMap:    cfg.InstrumentMap,
		speedUpdateChan:  make(chan struct{}, 1),
	}
}

func (d *DBBacktestSource) Connect(ctx context.Context) error {
	d.ctx, d.cancel = context.WithCancel(ctx)

	config, err := pgxpool.ParseConfig(d.dbConnString)
	if err != nil {
		return fmt.Errorf("failed to parse connection string: %w", err)
	}

	config.MaxConns = 2
	config.MinConns = 2
	config.MaxConnLifetime = 1 * time.Hour

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("failed to open database pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("failed to ping production database: %w", err)
	}

	d.db = pool
	logger.Infof("Successfully connected to TimescaleDB source via pgxpool for date: %s", d.date.Format("2006-01-02"))
	return nil
}

func (d *DBBacktestSource) SetSpeedFactor(factor float64) {
	d.mu.Lock()
	d.speedFactor = factor
	d.mu.Unlock()

	select {
	case d.speedUpdateChan <- struct{}{}:
	default:
	}
}

func (d *DBBacktestSource) GetSpeedFactor() float64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.speedFactor
}

// Internal helper struct to hold loaded depth snapshots in RAM
type cachedDepth struct {
	Timestamp       time.Time
	InstrumentToken uint32
	Side            string
	Level           models.DepthLevel
}

func (d *DBBacktestSource) ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error {
	startTime := d.date.UTC()
	endTime := startTime.Add(24 * time.Hour)

	// ============================================================================
	// PHASE 1: PRE-WARM RAM CACHE (Eager fetch all records into local slices)
	// ============================================================================
	logger.Infof("[RAM Warmup] Fetching ticks and depth items from TimescaleDB into local RAM cache...")

	// 1. Fetch and load all Ticks into RAM
	tickQuery := `
       SELECT timestamp, instrument_token, stock_name, last_price, last_traded_quantity, 
              average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, 
              open, high, low, close, change
       FROM live_ticks
       WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
       ORDER BY timestamp ASC;`

	tickRows, err := d.db.Query(ctx, tickQuery, startTime, endTime, d.instrumentTokens)
	if err != nil {
		return fmt.Errorf("error querying live_ticks: %w", err)
	}

	var tickCache []models.TickData
	for tickRows.Next() {
		var t models.TickData
		if err := tickRows.Scan(
			&t.Timestamp, &t.InstrumentToken, &t.StockName, &t.LastPrice,
			&t.LastTradedQuantity, &t.AverageTradedPrice, &t.CumulativeVolume,
			&t.TotalBuyQuantity, &t.TotalSellQuantity, &t.Open, &t.High,
			&t.Low, &t.Close, &t.Change,
		); err != nil {
			tickRows.Close()
			return err
		}
		if t.StockName == "" {
			t.StockName = d.instrumentMap[t.InstrumentToken]
		}
		tickCache = append(tickCache, t)
	}
	tickRows.Close()

	// 2. Fetch and load all Order Book Depth items into RAM
	depthQuery := `
       SELECT timestamp, instrument_token, side, price, quantity, orders
       FROM live_order_depth
       WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
       ORDER BY timestamp ASC;`

	depthRows, err := d.db.Query(ctx, depthQuery, startTime, endTime, d.instrumentTokens)
	if err != nil {
		return fmt.Errorf("error querying live_order_depth: %w", err)
	}

	var depthCache []cachedDepth
	for depthRows.Next() {
		var cd cachedDepth
		if err := depthRows.Scan(&cd.Timestamp, &cd.InstrumentToken, &cd.Side, &cd.Level.Price, &cd.Level.Quantity, &cd.Level.Orders); err != nil {
			depthRows.Close()
			return err
		}
		depthCache = append(depthCache, cd)
	}
	depthRows.Close()

	logger.Infof("[RAM Warmup] Completed! Loaded %d ticks and %d depths into volatile structures.", len(tickCache), len(depthCache))

	// ============================================================================
	// PHASE 2: ULTRA-SPEED IN-MEMORY REPLAY LOOP
	// ============================================================================
	currentDepths := make(map[uint32]*models.OrderDepth)
	for _, token := range d.instrumentTokens {
		currentDepths[token] = &models.OrderDepth{
			Buy:  make([]models.DepthLevel, 0),
			Sell: make([]models.DepthLevel, 0),
		}
	}

	depthIdx := 0
	var anchorMarketTime time.Time
	var anchorRealTime time.Time
	var activeSpeedFactor float64

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	// Replay loop streams entirely from the loaded RAM cache slices
	for _, tick := range tickCache {

		// Synchronize depth updates up to the current tick's timestamp boundary out of RAM array
		for depthIdx < len(depthCache) && !depthCache[depthIdx].Timestamp.After(tick.Timestamp) {
			cd := depthCache[depthIdx]
			targetBook := currentDepths[cd.InstrumentToken]

			if targetBook != nil {
				if cd.Side == "buy" {
					targetBook.Buy = append(targetBook.Buy, cd.Level)
				} else {
					targetBook.Sell = append(targetBook.Sell, cd.Level)
				}
			}
			depthIdx++
		}

		if book, exists := currentDepths[tick.InstrumentToken]; exists {
			tick.Depth = *book
		}

		select {
		case tickChan <- tick:
		case <-ctx.Done():
			return ctx.Err()
		}

		// ⚡ MAXIMUM VELOCITY SHORT-CIRCUIT
		if d.GetSpeedFactor() == -99 {
			continue
		}

		// Pacing calculation block
		for {
			currentSpeed := d.GetSpeedFactor()
			if currentSpeed == 0 {
				break
			}

			if anchorMarketTime.IsZero() || currentSpeed != activeSpeedFactor {
				anchorMarketTime = tick.Timestamp
				anchorRealTime = time.Now()
				activeSpeedFactor = currentSpeed
				break
			}

			marketDuration := tick.Timestamp.Sub(anchorMarketTime)
			var simulatedDuration time.Duration

			if currentSpeed > 0 {
				simulatedDuration = time.Duration(float64(marketDuration) / currentSpeed)
			} else {
				break
			}

			targetRealTime := anchorRealTime.Add(simulatedDuration)
			waitDuration := targetRealTime.Sub(time.Now())

			if waitDuration <= 0 {
				break
			}

			timer.Reset(waitDuration)

			select {
			case <-timer.C:
				break
			case <-d.speedUpdateChan:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
			break
		}
	}

	return ErrBacktestFinished
}

func (d *DBBacktestSource) Close() error {
	if d.cancel != nil {
		d.cancel()
	}
	if d.db != nil {
		d.db.Close()
	}
	return nil
}

func (d *DBBacktestSource) Type() SourceType           { return SourceBacktest }
func (d *DBBacktestSource) Subscribe(t []uint32) error { return nil }
