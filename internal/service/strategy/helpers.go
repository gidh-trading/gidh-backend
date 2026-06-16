package strategy

import (
	"time"

	"gidh-backend/internal/service/models"
)

// getOrInitializeState extracts or boots context tracking registers cleanly
func (e *Engine) getOrInitializeState(symbol string) *InstrumentState {
	state, exists := e.Registry[symbol]
	if !exists {
		profile := e.profiles[symbol]
		state = &InstrumentState{
			StockName:         symbol,
			Profile:           profile,
			CurrentSetupPhase: PhaseNeutral,
			BarHistory:        make(map[string][]*models.Bar),
		}
		e.Registry[symbol] = state
	}
	return state
}

// calculateNormalizedDistance determines the signed percentage gap relative to asset ADR percentage limits
func (e *Engine) calculateNormalizedDistance(price float64, vwap float64, profile *models.InstrumentProfile) float64 {
	if vwap <= 0 {
		return 0.0
	}

	// 1. Derive the standard raw distance from VWAP as a percentage value (e.g., +0.56%)
	rawDistancePercentage := ((price - vwap) / vwap) * 100.0

	// 2. Normalize distance using your profile data frame properties
	// Since profile.ADRPct is stored as a raw layout float (e.g., 2.80 representing 2.80%),
	// we divide directly by that threshold percentage capacity.
	if profile != nil && profile.ADRPct > 0 {
		return rawDistancePercentage / profile.ADRPct
	}

	return rawDistancePercentage
}

// appendAndPruneHistory inserts closed bars into isolated lookback buffers for strategy card evaluations
func (e *Engine) appendAndPruneHistory(state *InstrumentState, bar *models.Bar) {
	timeframe := bar.Timeframe
	state.BarHistory[timeframe] = append(state.BarHistory[timeframe], bar)

	maxBars := int(e.MaxBarLookback / time.Minute)
	if maxBars <= 0 {
		maxBars = 100
	}
	if len(state.BarHistory[timeframe]) > maxBars {
		state.BarHistory[timeframe] = state.BarHistory[timeframe][1:]
	}
}

// updateSignalPhaseAndExtensions manages execution anchors inside live signal generation passes
func (e *Engine) updateSignalPhaseAndExtensions(state *InstrumentState, currentSide string, averagePrice float64, netQty int) {
	if currentSide == "FLAT" || currentSide == "" || netQty == 0 {
		state.CurrentSetupPhase = PhaseNeutral
		state.EntryVwapAnchor = 0.0
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
		if state.EntryVwapAnchor == 0 {
			state.EntryVwapAnchor = state.LiveSessionVWAP
		}
	}
}

// calculateActivePnLState updates current and peak portfolio performance tracking deltas
func (e *Engine) calculateActivePnLState(state *InstrumentState, bar *models.Bar) {
	if state.CurrentSetupPhase != PhaseActiveTrade || state.ActiveSide == "FLAT" || state.ActiveSide == "" {
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		return
	}

	if state.ActiveSide == "LONG" {
		state.CurrentPnL = bar.Close - state.ActiveAvgPrice
	} else if state.ActiveSide == "SHORT" {
		state.CurrentPnL = state.ActiveAvgPrice - bar.Close
	}

	if state.CurrentPnL > state.PeakPnL {
		state.PeakPnL = state.CurrentPnL
	}
}

// Helper method to write strategy logs into the database out of state metrics safely.
// This executes asynchronously to protect the tick matching engine from latency penalties.
func (e *Engine) logStrategyDecision(state *InstrumentState, symbol string, action string, reason string, qty int, marketTime time.Time) {
	if e.dbWriter == nil {
		return
	}

	// If no trade ID exists yet (Entry), generate a unique grouping string for this lifecycle
	if state.CurrentTradeID == "" {
		state.CurrentTradeID = symbol + "-" + marketTime.Format("20060102-150405.000")
	}

	// Build context metadata map to figure out where strategy went wrong later
	snapshot := map[string]interface{}{
		"latest_price":      state.LatestPrice,
		"live_session_vwap": state.LiveSessionVWAP,
		"phase":             state.CurrentSetupPhase,
	}

	txLog := models.StrategyTransaction{
		TradeID:        state.CurrentTradeID,
		StrategyName:   e.ActiveStrategy.Name(),
		Instrument:     symbol,
		ActionType:     action,
		Price:          state.LatestPrice,
		Quantity:       float64(qty),
		ExecutionTime:  marketTime,
		TriggerReason:  reason,
		CurrentPnL:     state.CurrentPnL,
		PeakPnL:        state.PeakPnL,
		MarketSnapshot: snapshot,
	}

	// Persist asynchronously via DBWriter handle standard connection thread pool
	go e.dbWriter.PersistStrategyTransaction(txLog)

	// Clear out identifier track trace if this action closed the position lifecycle
	if action == "EXIT_LONG" || action == "EXIT_SHORT" {
		state.CurrentTradeID = ""
	}
}
