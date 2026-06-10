package strategy

import (
	"time"

	"gidh-backend/internal/service/models"
)

func NewEngine(barLookback time.Duration, profiles map[string]*models.InstrumentProfile, completeHook func(log *OptimizationTradeLog)) *Engine {
	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   NewInstitutionalLedgerStrategy(),
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		OnTradeCompleted: completeHook,
	}
}

func (e *Engine) IngestClosedBar(bar *models.Bar) {
	if bar == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrCreateState(bar.StockName)

	state.updateBasicMetrics(bar)
	state.calculateNormalizedDistance()
	state.appendAndPruneHistory(bar, e.MaxBarLookback)

	if state.isBeforeMarketOpen(bar.Timestamp) {
		return
	}

	state.trackVwapAcceptance(bar)
	state.updateVolumeLedger(bar)
}

func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrCreateState(enrichedTick.Raw.StockName)
	state.updateLiveTickData(enrichedTick)

	// Time constraints are completely removed from the stream pass-through gateway logic.
	if currentSide == "FLAT" || currentSide == "" {
		return e.evaluateLiveEntries(state)
	}
	return e.evaluateLiveExits(state, currentSide, averagePrice, netQty)
}

func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}
	e.mu.Unlock()

	if currentSide == "FLAT" || currentSide == "" {
		return e.executeFlatDiscovery(symbol, state)
	}
	return e.executeActivePositionManagement(symbol, state, currentSide, averagePrice, netQty)
}
