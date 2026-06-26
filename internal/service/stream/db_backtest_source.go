package stream

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

type DBBacktestSource struct {
	db               *sql.DB
	dbConnString     string
	date             time.Time
	speedFactor      float64
	instrumentTokens []uint32
	instrumentMap    map[uint32]string // mapping token -> stock_name

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

	db, err := sql.Open("postgres", d.dbConnString)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	// Optimize connection pool settings for large sequential reads
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	db.SetConnMaxLifetime(1 * time.Hour)

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return fmt.Errorf("failed to ping production database: %w", err)
	}

	d.db = db
	logger.Infof("Successfully connected to TimescaleDB source for date: %s", d.date.Format("2006-01-02"))
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

func (d *DBBacktestSource) ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error {
	startTime := d.date.UTC()
	endTime := startTime.Add(24 * time.Hour)

	// 1. Fetch Ticks ordered by timestamp across all subscribed tokens
	tickQuery := `
		SELECT timestamp, instrument_token, stock_name, last_price, last_traded_quantity, 
		       average_traded_price, volume_traded, total_buy_quantity, total_sell_quantity, 
		       open, high, low, close, change
		FROM live_ticks
		WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
		ORDER BY timestamp ASC;`

	tickRows, err := d.db.QueryContext(ctx, tickQuery, startTime, endTime, d.instrumentTokens)
	if err != nil {
		return fmt.Errorf("error querying live_ticks: %w", err)
	}
	defer tickRows.Close()

	// 2. Fetch Order Book Depth snapshots ordered by timestamp across all subscribed tokens
	depthQuery := `
		SELECT timestamp, instrument_token, side, price, quantity, orders
		FROM live_order_depth
		WHERE timestamp >= $1 AND timestamp < $2 AND instrument_token = ANY($3)
		ORDER BY timestamp ASC;`

	depthRows, err := d.db.QueryContext(ctx, depthQuery, startTime, endTime, d.instrumentTokens)
	if err != nil {
		return fmt.Errorf("error querying live_order_depth: %w", err)
	}
	defer depthRows.Close()

	// Maintain a real-time tracking map for market depths across instruments
	currentDepths := make(map[uint32]*models.OrderDepth)
	for _, token := range d.instrumentTokens {
		currentDepths[token] = &models.OrderDepth{
			Buy:  make([]models.DepthLevel, 0),
			Sell: make([]models.DepthLevel, 0),
		}
	}

	// Helper to load the next depth record from our database cursor stream
	var nextDepthTS time.Time
	var nextDepthToken uint32
	var pendingDepthItem *struct {
		Side  string
		Level models.DepthLevel
	}

	advanceDepthStream := func() bool {
		if pendingDepthItem != nil {
			return true
		}

		var side string
		var price float64
		var qty int64
		var orders int

		if depthRows.Next() {
			err := depthRows.Scan(&nextDepthTS, &nextDepthToken, &side, &price, &qty, &orders)
			if err != nil {
				return false
			}
			pendingDepthItem = &struct {
				Side  string
				Level models.DepthLevel
			}{
				Side:  side,
				Level: models.DepthLevel{Price: price, Quantity: qty, Orders: orders},
			}
			return true
		}
		return false
	}

	// Track baseline timeframes for simulation pacing
	var anchorMarketTime time.Time
	var anchorRealTime time.Time
	var activeSpeedFactor float64

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	// Main Tick Streaming Loop
	for tickRows.Next() {
		var tick models.TickData
		err := tickRows.Scan(
			&tick.Timestamp, &tick.InstrumentToken, &tick.StockName, &tick.LastPrice,
			&tick.LastTradedQuantity, &tick.AverageTradedPrice, &tick.CumulativeVolume,
			&tick.TotalBuyQuantity, &tick.TotalSellQuantity, &tick.Open, &tick.High,
			&tick.Low, &tick.Close, &tick.Change,
		)
		if err != nil {
			return err
		}

		// Ensure the stock name map updates accurately if missing from production rows
		if tick.StockName == "" {
			tick.StockName = d.instrumentMap[tick.InstrumentToken]
		}

		// Pull and update all historical order depths up to the current tick's timestamp boundary
		for advanceDepthStream() && !nextDepthTS.After(tick.Timestamp) {
			targetBook := currentDepths[nextDepthToken]
			if targetBook == nil {
				pendingDepthItem = nil
				continue
			}

			// Clean book up if it's a completely fresh snapshot sequence group
			// Note: Adjust tracking logic if you emit updates instead of full frames
			if pendingDepthItem.Side == "buy" {
				targetBook.Buy = append(targetBook.Buy, pendingDepthItem.Level)
			} else {
				targetBook.Sell = append(targetBook.Sell, pendingDepthItem.Level)
			}
			pendingDepthItem = nil
		}

		// Clone and assign synced book context state to the current tick object
		if book, exists := currentDepths[tick.InstrumentToken]; exists {
			tick.Depth = *book
		}

		// Deliver the complete tick object to pipeline processors
		select {
		case tickChan <- tick:
		case <-ctx.Done():
			return ctx.Err()
		}

		// Pacing and execution speed configuration block (reused from your original file logic)
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

			if currentSpeed == -99 {
				simulatedDuration = marketDuration
			} else if currentSpeed > 0 {
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
		return d.db.Close()
	}
	return nil
}

func (d *DBBacktestSource) Type() SourceType           { return SourceBacktest }
func (d *DBBacktestSource) Subscribe(t []uint32) error { return nil }
