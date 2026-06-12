package strategy

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

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
	ledgerStrategyCard := NewInstitutionalLedgerStrategy()
	timeRouterWrapper := NewTimeBasedRouter(ledgerStrategyCard)

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   timeRouterWrapper,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		OnTradeCompleted: completeHook,
	}
}

// IngestClosedBar caches historical timeframes and computes metrics upon bar close
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	if e.isBeforeMarketOpen(bar) {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrInitializeState(bar.StockName)

	// 1. 🛡️ MORNING HOOD FILTER: Track boundaries strictly without polluting active state variables
	currentTimeHM := (bar.Timestamp.Hour() * 100) + bar.Timestamp.Minute()
	if currentTimeHM <= 915 {
		// Hard stop to protect the sequential counters and efficiency metrics from morning noise
		return
	}

	// 2. POST-09:20 AM: Safe, clean-slate metric bookkeeping executions
	e.updateCoreBarMetrics(state, bar) // Updates metrics and calculates the unified signed efficiency
	e.trackVwapAcceptance(state, bar)  // Increments consecutive counters cleanly

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	e.appendAndPruneHistory(state, bar)
}

// UpdateContext updates real-time tracking metrics and evaluates active trailing locks
// and dynamic tick-level entries/exits live on every incoming tick.
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state := e.getOrInitializeState(symbol)

	e.updateCoreTickMetrics(state, enrichedTick.Raw)

	// Hard-lock tick execution routing entirely during the opening range window
	tickTime := enrichedTick.Raw.Timestamp
	marketHM := (tickTime.Hour() * 100) + tickTime.Minute()
	if marketHM <= 915 {
		e.mu.Unlock()
		return "HOLD"
	}

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	// Hardcoded strategy parameters
	adrMultiplier := 0.05
	isFlatNow := currentSide == "FLAT" || currentSide == ""

	if isFlatNow {
		return e.evaluateFlatTickEntry(state, adrMultiplier)
	}

	return e.evaluateActiveTickPosition(state, symbol, currentSide, averagePrice, netQty, adrMultiplier)
}

// GenerateSignal handles execution tracking and logs freeze-frame microstructural metrics
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	// Safety check: Explicitly ignore signaling routines if the bar cache falls within the opening room limits
	if state.LastUpdated.Hour() == 9 && state.LastUpdated.Minute() <= 15 {
		e.mu.Unlock()
		return "HOLD"
	}

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)
	e.mu.Unlock()

	if state.CurrentSetupPhase == PhaseNeutral {
		return e.processNeutralSignalRoute(symbol, state)
	}

	return e.processActiveSignalRoute(symbol, state, currentSide, averagePrice, netQty)
}
