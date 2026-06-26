package strategy

import (
	"fmt"
	"time"

	"gidh-backend/internal/service/models"
)

// getOrInitializeState extracts or initializes a composite context tracking registration block
func (e *Engine) getOrInitializeState(symbol, strategyName string) *InstrumentState {
	compositeKey := fmt.Sprintf("%s_%s", symbol, strategyName)
	state, exists := e.Registry[compositeKey]
	if !exists {
		state = &InstrumentState{
			StockName:          symbol,
			ActiveStrategyName: strategyName,
			MaxPnL:             0.0,
			Profile:            e.profiles[symbol],
			VwapPercentile:     e.vwapPercentiles[symbol],
			BarHistory:         make(map[string][]*models.Bar),
			StrategyHistory:    make(map[string]StrategyStats),
		}
		e.Registry[compositeKey] = state
	}

	return state
}

// calculateNormalizedDistance determines the signed percentage gap relative to asset ADR percentage limits
func (e *Engine) calculateNormalizedDistance(price float64, vwap float64, profile *models.InstrumentProfile) float64 {
	if vwap <= 0 {
		return 0.0
	}

	rawDistancePercentage := ((price - vwap) / vwap) * 100.0
	if profile != nil && profile.ADRPct > 0 {
		return rawDistancePercentage / profile.ADRPct
	}

	return rawDistancePercentage
}

// appendAndPruneHistory inserts closed bars into lookback buffers
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

// updateSignalPhaseAndExtensions manages execution anchors inside signal updates
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

// calculateActivePnLState updates current and peak performance tracking counters
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

// logStrategyDecision writes strategy logs asynchronously into the data layer
func (e *Engine) logStrategyDecision(state *InstrumentState, symbol string, action string, reason string, qty int, marketTime time.Time) {
	if e.dbWriter == nil {
		return
	}

	if state.CurrentTradeID == "" {
		state.CurrentTradeID = symbol + "-" + marketTime.Format("20060102-150405.000")
	}

	snapshot := map[string]interface{}{
		"latest_price":      state.LatestPrice,
		"live_session_vwap": state.LiveSessionVWAP,
		"phase":             state.CurrentSetupPhase,
	}

	txLog := models.StrategyTransaction{
		TradeID:        state.CurrentTradeID,
		StrategyName:   state.ActiveStrategyName,
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

	go e.dbWriter.PersistStrategyTransaction(txLog)

	if action == "EXIT_LONG" || action == "EXIT_SHORT" {
		state.CurrentTradeID = ""
	}
}

func (e *Engine) GetADRBounds(state *InstrumentState) (ceiling float64, floor float64, ok bool) {
	if state == nil || state.Profile == nil || state.Profile.ADRPct <= 0 || state.SessionOpen <= 0 {
		return 0, 0, false
	}

	adrPoints := state.SessionOpen * (state.Profile.ADRPct / 100.0)
	ceilingPrice := state.SessionLow + adrPoints
	floorPrice := state.SessionHigh - adrPoints

	return ceilingPrice, floorPrice, true
}

func mathAbs(v float64) float64 {
	if v < 0 {
		return -v
	}
	return v
}
