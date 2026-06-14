package strategy

import (
	"context"
	"sync"
	"time"

	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
)

const (
	EfficiencySlopeLookback = 4 // Lookback window to calculate the linear regression line
)

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile
	dbWriter       *writer.DBWriter

	// --- 📊 Optimization Logger Integrations ---
	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog)
}

// NewEngine accepts pre-loaded profiles map, trade logging callback hooks, and the dbWriter package.
func NewEngine(
	barLookback time.Duration,
	profiles map[string]*models.InstrumentProfile,
	dbW *writer.DBWriter,
	completeHook func(log *OptimizationTradeLog),
) *Engine {
	ledgerStrategyCard := NewInstitutionalLedgerStrategy()
	timeRouterWrapper := NewTimeBasedRouter(ledgerStrategyCard)

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   timeRouterWrapper,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		dbWriter:         dbW,
		OnTradeCompleted: completeHook,
	}
}

// IngestClosedBar caches historical timeframes and computes metrics upon bar close
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrInitializeState(bar.StockName)

	e.updateCoreBarMetrics(state, bar)
	e.trackVwapAcceptance(state, bar)

	// Update running percentage of time spent above VWAP
	state.TotalSessionBars++
	if bar.Close > bar.VWAP {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1)) + 1.0
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	} else {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1))
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	}

	// 1. Process ledger calculations first to derive raw underlying dynamics
	e.ProcessClosedBarLedger(state, bar)

	// 2. Compute Net Efficiency and roll it into the history buffer
	state.NetEfficiency = bar.Analytics.NetEfficiency
	state.NetEfficiencyHistory = append(state.NetEfficiencyHistory, state.NetEfficiency)

	// Prune history slice to prevent memory exhaustion
	if len(state.NetEfficiencyHistory) > EfficiencySlopeLookback {
		state.NetEfficiencyHistory = state.NetEfficiencyHistory[1:]
	}

	// Calculate current Slope trend over the window
	state.NetEfficiencySlope = CalculateLinearRegressionSlope(state.NetEfficiencyHistory)

	// Track Peak PnL for active optimization metrics
	if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists {
		var currentUnrealized float64
		if tradeLog.TradeSide == "LONG" {
			currentUnrealized = (bar.Close - tradeLog.EntryPrice)
		} else if tradeLog.TradeSide == "SHORT" {
			currentUnrealized = (tradeLog.EntryPrice - bar.Close)
		}
		if currentUnrealized > tradeLog.PeakPnLINR {
			tradeLog.PeakPnLINR = currentUnrealized
		}
	}

	// 3. Assign cleaned metrics to analytics framework containers
	bar.Analytics.NetEfficiency = state.NetEfficiency
	bar.Analytics.NetEfficiencySlope = state.NetEfficiencySlope

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)
	e.appendAndPruneHistory(state, bar)

	if e.dbWriter != nil {
		e.dbWriter.AddBar(*bar)
	}
}

// UpdateContext updates real-time tracking metrics and evaluates live tick parameters
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state := e.getOrInitializeState(symbol)

	e.updateCoreTickMetrics(state, enrichedTick.Raw)
	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	e.mu.Unlock()

	if e.ActiveStrategy != nil {
		if isFlatNow {
			signal := e.ActiveStrategy.CheckEntry(state)
			if signal == "GO_LONG" || signal == "GO_SHORT" {
				e.LogOptimizationEntry(symbol, signal, state)
			}
			return signal
		}

		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			e.LogOptimizationExit(symbol, signal, state)
		}
		return signal
	}

	return "HOLD"
}

// GenerateSignal handles execution tracking and logs freeze-frame microstructural metrics
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)
	e.mu.Unlock()

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		return e.ActiveStrategy.CheckEntry(state)
	}
	return e.ActiveStrategy.CheckExit(state, currentSide)
}

// LogOptimizationEntry snapshots critical microstructural properties on signal execution
func (e *Engine) LogOptimizationEntry(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.ActiveTrades[symbol]; exists {
		return
	}

	tradeSide := "LONG"
	if signal == "GO_SHORT" {
		tradeSide = "SHORT"
	}

	strategyName := "Institutional_Ledger"
	if e.ActiveStrategy != nil {
		strategyName = e.ActiveStrategy.Name()
	}

	log := &OptimizationTradeLog{
		Symbol:            symbol,
		StrategyName:      strategyName,
		TradeSide:         tradeSide,
		EntryTimestamp:    time.Now(),
		EntryPrice:        state.LatestPrice,
		EntryVwap:         state.LiveSessionVWAP,
		EntryVwapDistance: state.NormalizedVwapDistance,
		CreatedAt:         time.Now(),
	}

	e.ActiveTrades[symbol] = log
}

// LogOptimizationExit compiles realization analytics and saves records via a direct database transaction
func (e *Engine) LogOptimizationExit(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	tradeLog, exists := e.ActiveTrades[symbol]
	if !exists {
		e.mu.Unlock()
		return
	}
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	tradeLog.ExitTimestamp = time.Now()
	tradeLog.ExitPrice = state.LatestPrice
	tradeLog.ExitReason = signal

	var finalPnL float64
	if tradeLog.TradeSide == "LONG" {
		finalPnL = tradeLog.ExitPrice - tradeLog.EntryPrice
	} else {
		finalPnL = tradeLog.EntryPrice - tradeLog.ExitPrice
	}
	tradeLog.FinalPnLINR = finalPnL

	go func(logRecord *OptimizationTradeLog) {
		pool := db.GetPool()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Pass clean zero values/placeholders for deleted micro-metrics inside the DB writer contract
		err := db.LogStrategyOptimizationTrade(
			ctx, pool, logRecord.Symbol, logRecord.StrategyName, logRecord.TradeSide,
			logRecord.MinutesSinceOpen, logRecord.EntryTimestamp, logRecord.EntryPrice,
			logRecord.EntryVwap, 0, 0, 0.0, logRecord.EntryVwapDistance, logRecord.ExitTimestamp,
			logRecord.ExitPrice, logRecord.ExitReason, logRecord.FinalPnLINR, logRecord.PeakPnLINR,
		)
		if err != nil {
			logger.Errorf("🚨 Optimization Engine direct write failed for %s: %v", logRecord.Symbol, err)
		}
	}(tradeLog)

	if e.OnTradeCompleted != nil {
		e.OnTradeCompleted(tradeLog)
	}
}

// --- 🛠️ REGRESSION UTILITY HELPERS ---

// CalculateLinearRegressionSlope returns the slope of historical data points over frames
func CalculateLinearRegressionSlope(values []float64) float64 {
	n := float64(len(values))
	if n < 2 {
		return 0.0 // Requires at least two points to establish a line
	}

	var sumX, sumY, sumXY, sumXX float64
	for i, y := range values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	denominator := (n * sumXX) - (sumX * sumX)
	if denominator == 0 {
		return 0.0
	}

	return (n*sumXY - sumX*sumY) / denominator
}
