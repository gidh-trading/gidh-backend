package strategy

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
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
	dbWriter       *writer.DBWriter // 🏛️ DBWriter holds direct primary orchestration assignment here now

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
	if e.isBeforeMarketOpen(bar) {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrInitializeState(bar.StockName)

	// 1. 🛡️ MORNING HOOD FILTER: Protect structural sequential configurations from raw context pollution
	currentTimeHM := (bar.Timestamp.Hour() * 100) + bar.Timestamp.Minute()
	if currentTimeHM <= 930 {
		return
	}

	// 2. Compute Core Parameters
	e.updateCoreBarMetrics(state, bar)
	e.trackVwapAcceptance(state, bar)

	// 3. 🧠 Process Modular Continuous Decay Ledger Assignments
	e.ProcessClosedBarLedger(state, bar)

	// 4. 📊 Populate the Enhanced Efficiency Fields inside Bar Objects for Dashboard Visualization
	bar.Analytics.Efficiency = state.Efficiency
	bar.Analytics.BullEfficient = state.Ledger.BullEfficient
	bar.Analytics.BearEfficient = state.Ledger.BearEfficient
	bar.Analytics.BullAbsorption = state.Ledger.BullAbsorption
	bar.Analytics.BearAbsorption = state.Ledger.BearAbsorption
	bar.Analytics.BullVacuum = state.Ledger.BullVacuum
	bar.Analytics.BearVacuum = state.Ledger.BearVacuum

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	e.appendAndPruneHistory(state, bar)

	// 5. 💾 Commit the Instrumented Candle Directly into Database Batch Buffer
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

	tickTime := enrichedTick.Raw.Timestamp
	marketHM := (tickTime.Hour() * 100) + tickTime.Minute()
	if marketHM <= 915 {
		e.mu.Unlock()
		return "HOLD"
	}

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

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

	currentTimeHM := (state.LastUpdated.Hour() * 100) + state.LastUpdated.Minute()
	if currentTimeHM <= 930 {
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
