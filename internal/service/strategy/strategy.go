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

	// 1. Core metric snapshots are captured on all bars (pre-market and normal session)
	state.updateBasicMetrics(bar)
	state.calculateNormalizedDistance()
	state.appendAndPruneHistory(bar, e.MaxBarLookback)

	// 🛑 PRE-MARKET PROTECTION GATING:
	// If the bar timestamp hour/minute is before the official 09:15 AM market open,
	// we do not let it pollute our trading counters or institutional ledger balance sheet.
	if state.isBeforeMarketOpen(bar.Timestamp) {
		return
	}

	// 2. Continuous session metric accumulation (Post-09:15 AM)
	state.trackVwapAcceptance(bar)
	state.updateVolumeLedger(bar)
}

func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrCreateState(enrichedTick.Raw.StockName)
	state.updateLiveTickData(enrichedTick)

	// 🛑 THE 09:30 AM - 03:00 PM TRADING WINDOW FILTER
	// If the current hour/minute is before 09:30 AM or after 03:00 PM IST,
	// we bypass trade entry discovery. We keep updating the ledger but return "HOLD" instantly.
	hm := (state.LastUpdated.Hour() * 100) + state.LastUpdated.Minute()
	if hm < 930 || hm >= 1500 {
		// If we are holding an active position past 3:00 PM, let evaluateLiveExits handle the closeout
		if currentSide != "FLAT" && currentSide != "" && hm >= 1500 {
			return e.evaluateLiveExits(state, currentSide, averagePrice, netQty)
		}
		return "HOLD"
	}

	// Midday Operations Phase (09:30 AM to 03:00 PM IST)
	if currentSide == "FLAT" || currentSide == "" {
		return e.evaluateLiveEntries(state)
	}
	return e.evaluateLiveExits(state, currentSide, averagePrice, netQty)
}

func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil || state.isBeforeSettleTime(state.LastUpdated) {
		e.mu.Unlock()
		return "HOLD"
	}
	e.mu.Unlock()

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		return e.executeFlatDiscovery(symbol, state)
	}
	return e.executeActivePositionManagement(symbol, state, currentSide, averagePrice, netQty)
}
