package stream

import (
	"context"
	"fmt"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
	kitemodels "github.com/zerodha/gokiteconnect/v4/models"
	kiteticker "github.com/zerodha/gokiteconnect/v4/ticker"
)

// LiveTickSource implements TickDataSource using Zerodha Kite Connect WebSocket
type LiveTickSource struct {
	ticker      *kiteticker.Ticker
	tickChan    chan<- models.TickData
	ctx         context.Context
	cancelCtx   context.CancelFunc
	stopOnce    sync.Once
	isConnected bool
	config      *LiveSourceConfig
	mu          sync.RWMutex
}

// LiveSourceConfig holds configuration for Kite Connect
type LiveSourceConfig struct {
	APIKey              string
	AccessToken         string
	Instruments         []uint32          // Tokens to subscribe
	InstrumentMap       map[uint32]string // Token to stock name mapping
	ReconnectMaxRetries int
	ReconnectInterval   int // in seconds
}

// NewLiveSource creates a new Kite Connect WebSocket source
func NewLiveSource(cfg *LiveSourceConfig) (*LiveTickSource, error) {
	if cfg == nil {
		return nil, fmt.Errorf("configuration is required")
	}
	if cfg.APIKey == "" || cfg.AccessToken == "" {
		return nil, fmt.Errorf("API_KEY and ACCESS_TOKEN are required")
	}
	if cfg.InstrumentMap == nil {
		cfg.InstrumentMap = make(map[uint32]string)
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = 5
	}
	if cfg.ReconnectMaxRetries <= 0 {
		cfg.ReconnectMaxRetries = 10
	}

	return &LiveTickSource{
		config: cfg,
	}, nil
}

// Connect initializes the Kite Ticker instance
func (l *LiveTickSource) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	logger.Info("Initializing Kite WebSocket connection")

	// Create cancellable context
	l.ctx, l.cancelCtx = context.WithCancel(ctx)

	// Initialize ticker
	l.ticker = kiteticker.New(l.config.APIKey, l.config.AccessToken)

	// Set reconnect policy
	if l.config.ReconnectMaxRetries > 0 {
		l.ticker.SetReconnectMaxRetries(l.config.ReconnectMaxRetries)
	}

	// Register event handlers
	l.ticker.OnError(l.onError)
	l.ticker.OnClose(l.onClose)
	l.ticker.OnConnect(l.onConnect)
	l.ticker.OnTick(l.onTick)
	l.ticker.OnReconnect(l.onReconnect)
	l.ticker.OnNoReconnect(l.onNoReconnect)

	// Updated: Convert Kite order to internal model for rich UI updates
	l.ticker.OnOrderUpdate(func(o kiteconnect.Order) {
		logger.Infof("[Kite] Order Update: %s -> %s (Filled: %d)", o.OrderID, o.Status, o.FilledQuantity)
	})

	logger.Info("Kite Ticker initialized successfully")
	return nil
}

// Subscribe registers interest in specific instruments
func (l *LiveTickSource) Subscribe(instrumentTokens []uint32) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.ticker == nil {
		return fmt.Errorf("ticker not initialized, call Connect first")
	}

	if len(instrumentTokens) == 0 {
		logger.Warn("No instrument tokens provided for subscription")
		return nil
	}

	l.config.Instruments = instrumentTokens

	if l.isConnected {
		logger.Infof("Setting WebSocket mode to FULL for %d instruments", len(instrumentTokens))
		if err := l.ticker.SetMode(kiteticker.ModeFull, instrumentTokens); err != nil {
			logger.Errorf("Failed to set mode: %v", err)
			return fmt.Errorf("failed to set WebSocket mode: %w", err)
		}
	} else {
		logger.Info("Subscription queued: will be applied once WebSocket connects")
	}

	return nil
}

// ReadTicks starts reading data and sending to channel
func (l *LiveTickSource) ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error {
	l.mu.Lock()
	l.tickChan = tickChan
	l.mu.Unlock()

	if l.ticker == nil {
		return fmt.Errorf("ticker not initialized, call Connect first")
	}

	logger.Info("Starting tick stream")

	go func() {
		l.ticker.Serve()
	}()

	<-ctx.Done()
	logger.Info("ReadTicks context cancelled")

	l.Close()
	return ctx.Err()
}

// Close terminates the connection
func (l *LiveTickSource) Close() error {
	var err error
	l.stopOnce.Do(func() {
		l.mu.Lock()
		defer l.mu.Unlock()

		logger.Info("Closing Kite WebSocket connection")

		if l.ticker != nil {
			l.ticker.SetReconnectMaxRetries(0)

			if len(l.config.Instruments) > 0 {
				if unsubErr := l.ticker.Unsubscribe(l.config.Instruments); unsubErr != nil {
					logger.Warnf("Error during unsubscribe: %v", unsubErr)
				}
			}

			l.ticker.Close()
			l.isConnected = false
		}

		if l.cancelCtx != nil {
			l.cancelCtx()
		}
	})

	return err
}

func (l *LiveTickSource) Type() SourceType {
	return SourceLive
}

func (l *LiveTickSource) IsConnected() bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.isConnected
}

// --- Event Handlers ---

func (l *LiveTickSource) onConnect() {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.isConnected = true
	logger.Info("WebSocket connected successfully")

	if len(l.config.Instruments) == 0 {
		logger.Warn("No instruments configured for subscription")
		return
	}

	if err := l.ticker.Subscribe(l.config.Instruments); err != nil {
		logger.Errorf("Failed to subscribe: %v", err)
		l.Close()
		return
	}

	if err := l.ticker.SetMode(kiteticker.ModeFull, l.config.Instruments); err != nil {
		logger.Errorf("Failed to set mode: %v", err)
		l.Close()
		return
	}
}

func (l *LiveTickSource) onTick(tick kitemodels.Tick) {
	stockName, found := l.config.InstrumentMap[tick.InstrumentToken]
	if !found {
		stockName = fmt.Sprintf("TOKEN_%d", tick.InstrumentToken)
	}

	tickData, err := l.convertTick(tick, stockName)
	if err != nil {
		logger.Warnf("Failed to convert tick for token %d: %v", tick.InstrumentToken, err)
		return
	}

	select {
	case l.tickChan <- tickData:
		logger.Debugf("Tick sent - Stock: %s, Price: %.2f", stockName, tickData.LastPrice)
	default:
		logger.Warnf("Tick channel full, dropping tick for %s", stockName)
	}
}

func (l *LiveTickSource) onError(err error) {
	logger.Errorf("WebSocket error: %v", err)
}

func (l *LiveTickSource) onClose(code int, reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.isConnected = false
	logger.Warnf("WebSocket connection closed - Code: %d, Reason: %s", code, reason)
}

func (l *LiveTickSource) onReconnect(attempt int, delay time.Duration) {
	logger.Infof("Attempting reconnect #%d in %.0f seconds", attempt, delay.Seconds())
}

func (l *LiveTickSource) onNoReconnect(attempt int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.isConnected = false
	logger.Errorf("Max reconnect attempts (%d) reached, giving up", attempt)
	l.Close()
}

func (l *LiveTickSource) convertTick(kiteTick kitemodels.Tick, stockName string) (models.TickData, error) {
	tickData := models.TickData{
		Timestamp:          time.Now().UTC(),
		InstrumentToken:    kiteTick.InstrumentToken,
		StockName:          stockName,
		LastPrice:          kiteTick.LastPrice,
		LastTradedQuantity: int64(kiteTick.LastTradedQuantity),
		AverageTradedPrice: kiteTick.AverageTradePrice,
		CumulativeVolume:   int64(kiteTick.VolumeTraded),
		TotalBuyQuantity:   int64(kiteTick.TotalBuyQuantity),
		TotalSellQuantity:  int64(kiteTick.TotalSellQuantity),
		Open:               kiteTick.OHLC.Open,
		High:               kiteTick.OHLC.High,
		Low:                kiteTick.OHLC.Low,
		Close:              kiteTick.OHLC.Close,
		Change:             kiteTick.NetChange,
		Depth: models.OrderDepth{
			Buy:  make([]models.DepthLevel, 0, len(kiteTick.Depth.Buy)),
			Sell: make([]models.DepthLevel, 0, len(kiteTick.Depth.Sell)),
		},
	}

	for _, item := range kiteTick.Depth.Buy {
		tickData.Depth.Buy = append(tickData.Depth.Buy, models.DepthLevel{
			Price: item.Price, Quantity: int64(item.Quantity), Orders: int(item.Orders),
		})
	}

	for _, item := range kiteTick.Depth.Sell {
		tickData.Depth.Sell = append(tickData.Depth.Sell, models.DepthLevel{
			Price: item.Price, Quantity: int64(item.Quantity), Orders: int(item.Orders),
		})
	}

	return tickData, nil
}
