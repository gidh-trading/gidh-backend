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

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   ledgerStrategyCard,
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

	if !state.HasInitializedGaps {
		e.initializeGapContext(state, bar)
	}

	e.updateCoreBarMetrics(state, bar)
	e.trackVwapAcceptance(state, bar)
	e.updateVolumeLedger(state, bar)

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

	if !state.HasInitializedGaps {
		// Only calculate gaps if it is the true opening market window
		tickTime := enrichedTick.Raw.Timestamp
		marketHM := (tickTime.Hour() * 100) + tickTime.Minute()

		if marketHM == 915 { // Strictly between 09:15:00 and 09:15:59 AM
			state.HasInitializedGaps = true
			if enrichedTick.Raw.Change > 0.0 {
				state.IsGapUp = true
				state.IsGapDown = false
			} else if enrichedTick.Raw.Change < 0.0 {
				state.IsGapDown = true
				state.IsGapUp = false
			}
		}
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

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)
	e.mu.Unlock()

	if state.CurrentSetupPhase == PhaseNeutral {
		return e.processNeutralSignalRoute(symbol, state)
	}

	return e.processActiveSignalRoute(symbol, state, currentSide, averagePrice, netQty)
}
