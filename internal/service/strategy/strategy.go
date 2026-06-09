package strategy

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

type InstrumentState struct {
	Symbol           string
	LastUpdated      time.Time
	LatestPrice      float64
	LatestDirection  models.DirectionState // e.g., "BULLISH_ABSORPTION"
	LatestVolumeRank int                   // Peak-locked rank (0, 1-7)
	LatestPriceRank  int                   // Candle body size percentile
	LiveSessionVWAP  float64               // Anchor line from exchange

	// --- 📈 Candlestick Structural Properties ---
	LatestBodySize  float64 // Absolute size of the candle body (|Close - Open|)
	LatestLowerWick float64 // Absolute size of the lower wick
	LatestWickRatio float64 // Lower wick size divided by total high-to-low range

	LastTradedBarTime time.Time

	// --- ⏱️ Strategy Time Constraints ---
	MinutesSinceOpen int // Minutes elapsed since market open (9:15 AM IST)

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase // e.g., "NEUTRAL" or "ACTIVE_TRADE"

	// --- Volatility Space ---
	NormalizedVwapDistance float64 // Distance from VWAP scaled by ADRPct

	// --- Memory Storage ---
	BarHistory map[string][]*models.Bar  // Holds historical closed bars
	Profile    *models.InstrumentProfile // Stores ADRPct and ADV constants
}

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile

	// --- 📊 Optimization Logger Integrations ---
	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog) // Hook for database saving / backtest logs
}

// NewEngine accepts pre-loaded profiles map and an active trade logging callback hook.
func NewEngine(barLookback time.Duration, profiles map[string]*models.InstrumentProfile, completeHook func(log *OptimizationTradeLog)) *Engine {
	// Initialize only our high-edge morning opening drive strategy card
	morningCard := NewMorningRankStrategy()
	timeBasedRouter := NewTimeBasedRouter(morningCard)

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   timeBasedRouter,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		OnTradeCompleted: completeHook,
	}
}

// IngestClosedBar caches historical timeframes and computes metrics upon bar close
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	if bar == nil {
		return
	}

	// 🛑 TIME GATE: Ignore anything before the official 9:15 AM market open
	currentTimeHM := (bar.Timestamp.Hour() * 100) + bar.Timestamp.Minute()
	if currentTimeHM < 915 {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	symbol := bar.StockName
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
			Profile:           e.profiles[symbol],
		}
		e.Registry[symbol] = state
	}

	// 1. Process Core Base Metric Assignments
	state.LastUpdated = bar.Timestamp
	state.LatestPrice = bar.Close
	state.LatestDirection = bar.Analytics.Direction
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank
	state.LiveSessionVWAP = bar.VWAP

	// 2. Compute Candle Geometric Proportions
	totalRange := bar.High - bar.Low
	state.LatestBodySize = math.Abs(bar.Close - bar.Open)

	if totalRange > 0 {
		candleBodyBottom := math.Min(bar.Open, bar.Close)
		state.LatestLowerWick = candleBodyBottom - bar.Low
		state.LatestWickRatio = state.LatestLowerWick / totalRange
	} else {
		state.LatestLowerWick = 0.0
		state.LatestWickRatio = 0.0
	}

	// 3. Compute Minutes Since Open (09:15 AM IST)
	state.MinutesSinceOpen = (bar.Timestamp.Hour() * 60) + bar.Timestamp.Minute() - 555

	// 4. Calculate Normalized Spatial Distance from VWAP
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	} else {
		state.NormalizedVwapDistance = 0.0
	}

	// 5. Append and prune historical frames
	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	barCutoff := bar.Timestamp.Add(-e.MaxBarLookback)
	validIdx := 0
	for i, historicalBar := range state.BarHistory[tf] {
		if historicalBar.Timestamp.Before(barCutoff) {
			validIdx = i + 1
		} else {
			break
		}
	}
	if validIdx > 0 {
		state.BarHistory[tf] = state.BarHistory[tf][validIdx:]
	}
}

// UpdateContext updates real-time tracking metrics between bar closes
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick) {
	e.mu.Lock()
	defer e.mu.Unlock()

	symbol := enrichedTick.Raw.StockName
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
			Profile:           e.profiles[symbol],
		}
		e.Registry[symbol] = state
	}

	rawTick := enrichedTick.Raw
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LiveSessionVWAP = rawTick.AverageTradedPrice

	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	}
}

// GenerateSignal handles execution tracking and logs freeze-frame microstructural metrics
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
	}
	e.mu.Unlock()

	// --- 🛑 TRACK A: FLAT EVALUATION (ENTRY DISCOVERY ROUTE) ---
	if state.CurrentSetupPhase == PhaseNeutral {
		e.mu.Lock()
		delete(e.ActiveTrades, symbol) // Ensure stale logs are cleaned up if order state got flat externally
		e.mu.Unlock()

		signal := e.ActiveStrategy.CheckEntry(state)

		if signal == "GO_LONG" || signal == "GO_SHORT" {
			tradeSide := "LONG"
			if signal == "GO_SHORT" {
				tradeSide = "SHORT"
			}

			if len(state.BarHistory["1m"]) > 0 {
				latestBar := state.BarHistory["1m"][len(state.BarHistory["1m"])-1]
				state.LastTradedBarTime = latestBar.Timestamp
			}

			// 📸 FREEZE-FRAME METRICS SNAPSHOT AT THE EXACT SECOND OF ENTRY
			e.mu.Lock()
			e.ActiveTrades[symbol] = &OptimizationTradeLog{
				Symbol:            symbol,
				StrategyName:      e.ActiveStrategy.Name(),
				TradeSide:         tradeSide,
				MinutesSinceOpen:  state.MinutesSinceOpen,
				EntryTimestamp:    state.LastUpdated,
				EntryPrice:        state.LatestPrice,
				EntryVwap:         state.LiveSessionVWAP,
				EntryVolumeRank:   state.LatestVolumeRank,
				EntryPriceRank:    state.LatestPriceRank,
				EntryWickRatio:    state.LatestWickRatio,
				EntryVwapDistance: state.NormalizedVwapDistance,
			}
			e.mu.Unlock()
			return signal
		}
		return "HOLD"
	}

	// --- 🎯 TRACK B: ACTIVE POSITION MANAGEMENT (EXIT ROUTE) ---
	if state.CurrentSetupPhase == PhaseActiveTrade {
		e.mu.RLock()
		openTrade, trackingTrade := e.ActiveTrades[symbol]
		e.mu.RUnlock()

		// Capture the exact bar/tick timestamp from our registry state
		marketExitTime := state.LastUpdated

		// 1. Evaluate Downside Protection (Stop Loss Check)
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "STOP_LOSS", averagePrice, netQty, marketExitTime)
			}
			return "EXIT_" + currentSide
		}

		// 2. Evaluate Target Limits (Take Profit Check)
		if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "TAKE_PROFIT", averagePrice, netQty, marketExitTime)
			}
			return "EXIT_" + currentSide
		}

		// 3. Evaluate Dynamic Trend Flipping Indicators
		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "DIRECTION_FLIP", averagePrice, netQty, marketExitTime)
			}
			return signal
		}
	}

	return "HOLD"
}

// dispatchCompleteLog finalizes numerical trade statistics and invokes the application writer hook
func (e *Engine) dispatchCompleteLog(symbol string, trade *OptimizationTradeLog, exitPrice float64, reason string, avgPrice float64, qty int, exitTime time.Time) {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	// Assign the actual historical/live tick data timestamp passed from state context
	trade.ExitTimestamp = exitTime
	trade.ExitPrice = exitPrice
	trade.ExitReason = reason

	multiplier := 1.0
	if trade.TradeSide == "SHORT" {
		multiplier = -1.0
	}

	// Calculate absolute cash gains/losses
	trade.FinalPnLINR = (exitPrice - avgPrice) * float64(qty) * multiplier

	if e.OnTradeCompleted != nil {
		go e.OnTradeCompleted(trade) // Dispatched asynchronously to protect processing loops from db latency
	}
}
