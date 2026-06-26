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

	logger.Infof("[High-Speed Engine] Launching prefetch pipelines for date: %s", d.date.Format("2006-01-02"))

	// Large bounded buffers to hold rows fetched ahead of time
	tickBuffer := make(chan models.TickData, 50000)
	depthBuffer := make(chan cachedDepth, 500000)

	var wg sync.WaitGroup
	var errPrefetch error
	var errOnce sync.Once

	setErr := func(e error) {
		errOnce.Do(func() {
			errPrefetch = e
		})
	}

	// 1. Worker: Prefetch Ticks
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(tickBuffer)

		tickQuery := `
			SELECT timestamp, instrument_token, stock_name, last_price, last_traded_quantity, 
			       average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, 
			       open, high, low, close, change
			FROM live_ticks
			WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
			ORDER BY timestamp ASC;`

		rows, err := d.db.Query(ctx, tickQuery, startTime, endTime, d.instrumentTokens)
		if err != nil {
			setErr(fmt.Errorf("ticks prefetch query failed: %w", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var t models.TickData
			if err := rows.Scan(
				&t.Timestamp, &t.InstrumentToken, &t.StockName, &t.LastPrice,
				&t.LastTradedQuantity, &t.AverageTradedPrice, &t.CumulativeVolume,
				&t.TotalBuyQuantity, &t.TotalSellQuantity, &t.Open, &t.High,
				&t.Low, &t.Close, &t.Change,
			); err != nil {
				setErr(fmt.Errorf("ticks scan failed: %w", err))
				return
			}
			if t.StockName == "" {
				t.StockName = d.instrumentMap[t.InstrumentToken]
			}

			select {
			case tickBuffer <- t:
			case <-ctx.Done():
				return
			}
		}
	}()

	// 2. Worker: Prefetch Order Book Depths
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(depthBuffer)

		depthQuery := `
			SELECT timestamp, instrument_token, side, price, quantity, orders
			FROM live_order_depth
			WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
			ORDER BY timestamp ASC;`

		rows, err := d.db.Query(ctx, depthQuery, startTime, endTime, d.instrumentTokens)
		if err != nil {
			setErr(fmt.Errorf("depth prefetch query failed: %w", err))
			return
		}
		defer rows.Close()

		for rows.Next() {
			var cd cachedDepth
			if err := rows.Scan(&cd.Timestamp, &cd.InstrumentToken, &cd.Side, &cd.Level.Price, &cd.Level.Quantity, &cd.Level.Orders); err != nil {
				setErr(fmt.Errorf("depth scan failed: %w", err))
				return
			}

			select {
			case depthBuffer <- cd:
			case <-ctx.Done():
				return
			}
		}
	}()

	// Reset / initialize state fresh for this day
	currentDepths := make(map[uint32]*models.OrderDepth)
	for _, token := range d.instrumentTokens {
		currentDepths[token] = &models.OrderDepth{
			Buy:  make([]models.DepthLevel, 0),
			Sell: make([]models.DepthLevel, 0),
		}
	}

	var anchorMarketTime time.Time
	var anchorRealTime time.Time
	var activeSpeedFactor float64

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	nextDepth, depthOpen := <-depthBuffer

	// ============================================================================
	// CONSUMER PLAYBACK LOOP
	// ============================================================================
	for tick := range tickBuffer {
		if errPrefetch != nil {
			return errPrefetch
		}

		// Reconstruct order depth timeline
		for depthOpen && !nextDepth.Timestamp.After(tick.Timestamp) {
			targetBook := currentDepths[nextDepth.InstrumentToken]
			if targetBook != nil {
				if nextDepth.Side == "buy" {
					targetBook.Buy = append(targetBook.Buy, nextDepth.Level)
				} else {
					targetBook.Sell = append(targetBook.Sell, nextDepth.Level)
				}
			}
			nextDepth, depthOpen = <-depthBuffer
		}

		if book, exists := currentDepths[tick.InstrumentToken]; exists {
			// Deep-copy the slice contents so subsequent mutations do not pollute historical ticks
			tick.Depth.Buy = append([]models.DepthLevel(nil), book.Buy...)
			tick.Depth.Sell = append([]models.DepthLevel(nil), book.Sell...)
		}

		select {
		case tickChan <- tick:
		case <-ctx.Done():
			return ctx.Err()
		}

		// ⚡ MAXIMUM VELOCITY SHORT-CIRCUIT (-99)
		if d.GetSpeedFactor() == -99 {
			continue
		}

		// PACING / REPLAY ENGINE BLOCK
		for {
			currentSpeed := d.GetSpeedFactor()

			if currentSpeed <= 0 {
				break
			}

			if anchorMarketTime.IsZero() || currentSpeed != activeSpeedFactor {
				anchorMarketTime = tick.Timestamp
				anchorRealTime = time.Now()
				activeSpeedFactor = currentSpeed
				break
			}

			marketDuration := tick.Timestamp.Sub(anchorMarketTime)
			simulatedDuration := time.Duration(float64(marketDuration) / currentSpeed)

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

	wg.Wait()
	if errPrefetch != nil {
		return errPrefetch
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
