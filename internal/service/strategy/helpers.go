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
	if state.CurrentSetupPhase != PhaseActiveTrade {
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		return
	}

	tradeLog, exists := e.ActiveTrades[bar.StockName]
	if !exists {
		return
	}

	if tradeLog.TradeSide == "LONG" {
		state.CurrentPnL = (bar.Close - tradeLog.EntryPrice)
	} else if tradeLog.TradeSide == "SHORT" {
		state.CurrentPnL = (tradeLog.EntryPrice - bar.Close)
	}

	if state.CurrentPnL > state.PeakPnL {
		state.PeakPnL = state.CurrentPnL
	}
	if state.PeakPnL > tradeLog.PeakPnLINR {
		tradeLog.PeakPnLINR = state.PeakPnL
	}
}

// evaluateExecutionRiskSafely processes strategy rules via true market timestamps
func (e *Engine) evaluateExecutionRiskSafely(state *InstrumentState, bar *models.Bar, marketTime time.Time) {
	if state.CurrentSetupPhase != PhaseActiveTrade || e.ActiveStrategy == nil {
		return
	}

	currentSide := "LONG"
	avgPrice := state.LatestPrice
	if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists {
		if tradeLog.TradeSide == "SHORT" {
			currentSide = "SHORT"
		}
		avgPrice = tradeLog.EntryPrice
	}

	if e.ActiveStrategy.CheckStopLoss(state, currentSide, avgPrice, 1) {
		go e.LogOptimizationExit(bar.StockName, "SAFETY_STOP_LOSS", state, marketTime)
	} else if e.ActiveStrategy.CheckTakeProfit(state, currentSide, avgPrice, 1) {
		go e.LogOptimizationExit(bar.StockName, "SAFETY_HIGH_CONF_TRAILING", state, marketTime)
	}
}
