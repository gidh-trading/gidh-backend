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
	DecayConstant   = 0.90
	TriggerLookback = 3
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

	state.TotalSessionBars++
	if bar.Close > bar.VWAP {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1)) + 1.0
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	} else {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1))
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	}

	e.ProcessClosedBarLedger(state, bar)

	// Update peak tracking performance parameters
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

	bar.Analytics.Efficiency = state.Efficiency
	bar.Analytics.BullEfficient = state.Ledger.BullEfficient
	bar.Analytics.BearEfficient = state.Ledger.BearEfficient

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)
	e.appendAndPruneHistory(state, bar)

	if e.dbWriter != nil {
		e.dbWriter.AddBar(*bar)
	}
}

// UpdateContext acts as the EXCLUSIVE SOLE OWNER of trade lifecycle tracking and order routing evaluation
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

		// If in an active position, evaluate dynamic trailing profit locks or strategy trend flips
		signal := e.ActiveStrategy.CheckExit(state, currentSide)

		// Fallback check: If strategy interface registers a trailing lock protection event, execute exit
		if signal == "HOLD" && e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
			if currentSide == "LONG" {
				signal = "EXIT_LONG"
			} else {
				signal = "EXIT_SHORT"
			}
		}

		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			e.LogOptimizationExit(symbol, signal, state)
		}
		return signal
	}

	return "HOLD"
}

// GenerateSignal handles execution tracking without re-evaluating strategy signals
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists {
		e.mu.Unlock()
		return "HOLD"
	}

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)
	e.mu.Unlock()

	// 🛡️ CRITICAL FIX: Generates direct routing pass-through without firing logging hooks.
	// This completely maps out and stops the duplicated ghost logs seen across your database tables.
	return "HOLD"
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
		EntryVolumeRank:   state.LatestVolumeRank,
		EntryPriceRank:    state.LatestPriceRank,
		EntryVwapDistance: state.NormalizedVwapDistance,
		CreatedAt:         time.Now(),
	}

	e.ActiveTrades[symbol] = log
}

// LogOptimizationExit compiles realization analytics and saves records via a direct database connection pool transaction
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

		err := db.LogStrategyOptimizationTrade(
			ctx, pool, logRecord.Symbol, logRecord.StrategyName, logRecord.TradeSide,
			logRecord.MinutesSinceOpen, logRecord.EntryTimestamp, logRecord.EntryPrice,
			logRecord.EntryVwap, logRecord.EntryVolumeRank, logRecord.EntryPriceRank,
			logRecord.EntryWickRatio, logRecord.EntryVwapDistance, logRecord.ExitTimestamp,
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
