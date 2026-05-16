package writer

import (
	"context"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type DBWriter struct {
	pool          *pgxpool.Pool
	config        *DBWriterConfig
	tickBatch     []models.TickData
	depthBatch    []DepthRecord
	barBatch      []models.Bar
	batchSize     int
	flushInterval time.Duration
	mu            sync.Mutex
	ctx           context.Context
	cancel        context.CancelFunc
	wg            sync.WaitGroup
}

type DepthRecord struct {
	Timestamp       time.Time
	InstrumentToken uint32
	StockName       string
	Side            string
	Price           float64
	Quantity        int64
	Orders          int
}

type DBWriterConfig struct {
	Pool               *pgxpool.Pool
	SkipDatabaseInsert bool
	BatchSize          int           // Number of rows per batch (default: 5000)
	FlushInterval      time.Duration // Flush interval (default: 5 seconds)
}

func NewDBWriter(cfg *DBWriterConfig) *DBWriter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 5000
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 5 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	writer := &DBWriter{
		pool:          cfg.Pool,
		config:        cfg,
		tickBatch:     make([]models.TickData, 0, cfg.BatchSize),
		depthBatch:    make([]DepthRecord, 0, cfg.BatchSize),
		barBatch:      make([]models.Bar, 0, cfg.BatchSize),
		batchSize:     cfg.BatchSize,
		flushInterval: cfg.FlushInterval,
		ctx:           ctx,
		cancel:        cancel,
	}

	// Start flush timer
	writer.wg.Add(1)
	go writer.flushTimer()

	return writer
}

func (w *DBWriter) AddTick(tick models.TickData) {

	w.mu.Lock()
	w.tickBatch = append(w.tickBatch, tick)

	if len(w.tickBatch) >= w.batchSize {
		// Swap the batch and flush in background
		batch := w.tickBatch
		w.tickBatch = make([]models.TickData, 0, w.batchSize)
		w.mu.Unlock() // Release lock immediately

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.insertTicksBatch(batch)
		}()
	} else {
		w.mu.Unlock()
	}
}

func (w *DBWriter) AddDepth(timestamp time.Time, token uint32, stockName string, side string, depth models.DepthLevel) {

	w.mu.Lock()
	w.depthBatch = append(w.depthBatch, DepthRecord{
		Timestamp:       timestamp,
		InstrumentToken: token,
		StockName:       stockName,
		Side:            side,
		Price:           depth.Price,
		Quantity:        depth.Quantity,
		Orders:          depth.Orders,
	})

	if len(w.depthBatch) >= w.batchSize {
		// Swap the batch and flush in background
		batch := w.depthBatch
		w.depthBatch = make([]DepthRecord, 0, w.batchSize)
		w.mu.Unlock() // Release lock immediately

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.insertDepthBatch(batch)
		}()
	} else {
		w.mu.Unlock()
	}
}

func (w *DBWriter) AddBar(bar models.Bar) {
	w.mu.Lock()
	w.barBatch = append(w.barBatch, bar)

	if len(w.barBatch) >= w.batchSize {
		batch := w.barBatch
		w.barBatch = make([]models.Bar, 0, w.batchSize)
		w.mu.Unlock()

		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			w.insertBarsBatch(batch)
		}()
	} else {
		w.mu.Unlock()
	}
}

func (w *DBWriter) PersistOrder(order models.OrderBookEntry) {
	if w.config.SkipDatabaseInsert {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// FIX: Use an independent $13 placeholder instead of re-using $10
	query := `
		INSERT INTO gidh_orders (
			order_id, symbol, product, side, order_type, quantity, 
			filled_qty, price, status, timestamp, target_price, sl_price, trading_date
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (order_id) DO UPDATE SET
			status = EXCLUDED.status,
			filled_qty = EXCLUDED.filled_qty,
			price = EXCLUDED.price;`

	// FIX: Pass order.Timestamp as the 13th argument (pgx maps time.Time to DATE natively)
	_, err := w.pool.Exec(ctx, query,
		order.OrderID, order.Symbol, "MIS", order.Side, order.OrderType, order.Qty,
		order.FilledQty, order.Price, order.Status, order.Timestamp,
		order.TargetPrice, order.StopLossPrice, order.Timestamp)

	if err != nil {
		logger.Errorf("DB Error persisting order %s: %v", order.OrderID, err)
	}
}

func (w *DBWriter) PersistPositionSnapshot(pos *models.Position, sessionTime time.Time) {
	if w.config.SkipDatabaseInsert {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := `
		INSERT INTO gidh_positions (
			trading_date, symbol, product, side, net_quantity, avg_price, realized_pnl, target_price, stop_loss_price, updated_at
		) VALUES ($1::date, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
		ON CONFLICT (trading_date, symbol, product) DO UPDATE SET
			side = EXCLUDED.side,
			net_quantity = EXCLUDED.net_quantity,
			avg_price = EXCLUDED.avg_price,
			realized_pnl = EXCLUDED.realized_pnl,
			target_price = EXCLUDED.target_price,
			stop_loss_price = EXCLUDED.stop_loss_price,
			updated_at = NOW();`

	_, err := w.pool.Exec(ctx, query,
		sessionTime, pos.Symbol, pos.Product, pos.Side, pos.NetQuantity,
		pos.AveragePrice, pos.RealizedPnL, pos.TargetPrice, pos.StopLossPrice)

	if err != nil {
		logger.Errorf("DB Error persisting position %s: %v", pos.Symbol, err)
	}
}

func (w *DBWriter) flushTimer() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.mu.Lock()
			tBatch := w.tickBatch
			dBatch := w.depthBatch
			bBatch := w.barBatch

			// Reset batch
			w.tickBatch = make([]models.TickData, 0, w.batchSize)
			w.depthBatch = make([]DepthRecord, 0, w.batchSize)
			w.barBatch = make([]models.Bar, 0, w.batchSize)

			w.mu.Unlock()

			if len(tBatch) > 0 {
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.insertTicksBatch(tBatch)
				}()
			}
			if len(dBatch) > 0 {
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.insertDepthBatch(dBatch)
				}()
			}
			if len(bBatch) > 0 {
				w.wg.Add(1)
				go func() {
					defer w.wg.Done()
					w.insertBarsBatch(bBatch)
				}()
			}

		case <-w.ctx.Done():
			return
		}
	}
}

func (w *DBWriter) insertTicksBatch(batch []models.TickData) {

	if w.config.SkipDatabaseInsert {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyCount, err := w.pool.CopyFrom(
		ctx,
		pgx.Identifier{"live_ticks"},
		[]string{
			"timestamp", "instrument_token", "stock_name", "last_price",
			"last_traded_quantity", "average_traded_price", "volume_traded",
			"total_buy_quantity", "total_sell_quantity", "open", "high",
			"low", "close", "change",
		},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			tick := batch[i]
			return []any{
				tick.Timestamp, tick.InstrumentToken, tick.StockName, tick.LastPrice,
				tick.LastTradedQuantity, tick.AverageTradedPrice, tick.CumulativeVolume,
				tick.TotalBuyQuantity, tick.TotalSellQuantity, tick.Open,
				tick.High, tick.Low, tick.Close, tick.Change,
			}, nil
		}),
	)

	if err != nil {
		logger.Errorf("Failed to insert ticks: %v", err)
	} else {
		logger.Debugf("Inserted %d ticks (background)", copyCount)
	}
}

func (w *DBWriter) insertDepthBatch(batch []DepthRecord) {

	if w.config.SkipDatabaseInsert {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyCount, err := w.pool.CopyFrom(
		ctx,
		pgx.Identifier{"live_order_depth"},
		[]string{"timestamp", "instrument_token", "stock_name", "side", "price", "quantity", "orders"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			record := batch[i]
			return []any{
				record.Timestamp, record.InstrumentToken, record.StockName,
				record.Side, record.Price, record.Quantity, record.Orders,
			}, nil
		}),
	)

	if err != nil {
		logger.Errorf("Failed to insert depth: %v", err)
	} else {
		logger.Debugf("Inserted %d depth records (background)", copyCount)
	}
}

func (w *DBWriter) insertBarsBatch(batch []models.Bar) {
	if w.config.SkipDatabaseInsert {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	copyCount, err := w.pool.CopyFrom(
		ctx,
		pgx.Identifier{"gidh_bars"},
		[]string{
			"timestamp", "instrument_token", "stock_name", "timeframe",
			"open", "high", "low", "close", "volume",
			"vwap", "poc", "vah", "val",
			"buy_volume", "sell_volume",
			"total_vol_energy", "buy_vol_energy", "sell_vol_energy",
			"total_rng_energy", "buy_rng_energy", "sell_rng_energy",
		},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			b := batch[i]
			return []any{
				b.Timestamp, b.InstrumentToken, b.StockName, b.Timeframe,
				b.Open, b.High, b.Low, b.Close, b.Volume,
				b.VWAP, b.POC, b.VAH, b.VAL,
				b.BuyVolume, b.SellVolume,
				b.TotalVolEnergy, b.BuyVolEnergy, b.SellVolEnergy,
				b.TotalRngEnergy, b.BuyRngEnergy, b.SellRngEnergy,
			}, nil
		}),
	)

	if err != nil {
		logger.Errorf("Failed to insert bars: %v", err)
	} else {
		logger.Debugf("Inserted %d closed bars (background)", copyCount)
	}
}

func (w *DBWriter) Close() {
	logger.Info("Closing DB writer, flushing remaining data...")

	w.cancel() // Stop the timer

	// Final sync flush for remaining data
	w.mu.Lock()
	tBatch := w.tickBatch
	dBatch := w.depthBatch
	bBatch := w.barBatch

	w.mu.Unlock()

	if len(tBatch) > 0 {
		w.insertTicksBatch(tBatch)
	}
	if len(dBatch) > 0 {
		w.insertDepthBatch(dBatch)
	}
	if len(bBatch) > 0 {
		w.insertBarsBatch(bBatch)
	}

	w.wg.Wait() // Wait for all background goroutines to finish
	logger.Info("DB writer closed")
}
