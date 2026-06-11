package strategy

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
)

// =========================================================================
// 🕒 TIME & INITIALIZATION HELPERS
// =========================================================================

func (e *Engine) isBeforeMarketOpen(bar *models.Bar) bool {
	if bar == nil {
		return true
	}
	currentTimeHM := (bar.Timestamp.Hour() * 100) + bar.Timestamp.Minute()
	return currentTimeHM < 915
}

func (e *Engine) getOrInitializeState(symbol string) *InstrumentState {
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
			Profile:           e.profiles[symbol],
		}
		e.Registry[symbol] = state
	}
	return state
}

// =========================================================================
// 📊 METRIC WRAPPER HELPERS
// =========================================================================

func (e *Engine) updateCoreBarMetrics(state *InstrumentState, bar *models.Bar) {
	state.LastUpdated = bar.Timestamp
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank

	if state.InitialOpenPrice == 0 {
		state.InitialOpenPrice = bar.Open
	}
}

func (e *Engine) updateCoreTickMetrics(state *InstrumentState, rawTick models.TickData) {
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LiveSessionVWAP = rawTick.AverageTradedPrice
}

func (e *Engine) trackVwapAcceptance(state *InstrumentState, bar *models.Bar) {
	if bar.Close > bar.VWAP {
		state.ConsecutiveClosesAboveVwap++
		state.ConsecutiveClosesBelowVwap = 0
		if state.ConsecutiveClosesAboveVwap >= 3 {
			state.IsVwapAcceptanceConfirmed = true
		}
	} else if bar.Close < bar.VWAP {
		state.ConsecutiveClosesBelowVwap++
		state.ConsecutiveClosesAboveVwap = 0
		if state.ConsecutiveClosesBelowVwap >= 3 {
			state.IsVwapAcceptanceConfirmed = true
		}
	}
}

func (e *Engine) updateVolumeLedger(state *InstrumentState, bar *models.Bar) {
	analytics := bar.Analytics
	if state.LatestVolumeRank >= 6 && state.LatestPriceRank >= 6 {
		switch analytics.Direction {
		case models.DirStrongBullish, models.DirBullish:
			state.BullishPushVolume += bar.Volume
		case models.DirStrongBearish, models.DirBearish:
			state.BearishPushVolume += bar.Volume
		}
	}
}

func (e *Engine) initializeGapContext(state *InstrumentState, firstBar *models.Bar) {
	if state.HasInitializedGaps {
		return
	}

	barHM := (firstBar.Timestamp.Hour() * 100) + firstBar.Timestamp.Minute()
	if barHM > 916 {
		return
	}

	state.InitialOpenPrice = firstBar.Open
	state.HasInitializedGaps = true

	// ⚠️ FIX: Update property reference from .Change to .ChangePct
	if firstBar.ChangePct > 0.0 {
		state.IsGapUp = true
		state.IsGapDown = false
	} else if firstBar.ChangePct < 0.0 {
		state.IsGapDown = true
		state.IsGapUp = false
	}
}

// =========================================================================
// 📐 MATH & VOLATILITY SCALE COMPUTATIONS
// =========================================================================

func (e *Engine) calculateNormalizedDistance(latestPrice, liveVwap float64, profile *models.InstrumentProfile) float64 {
	if liveVwap > 0 && profile != nil && profile.ADRPct > 0 {
		rawDistancePct := ((latestPrice - liveVwap) / liveVwap) * 100
		return rawDistancePct / profile.ADRPct
	}
	return 0.0
}

func (e *Engine) calculateCurrentExtension(state *InstrumentState) float64 {
	if state.EntryVwapAnchor > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawAnchorDistancePct := ((state.LatestPrice - state.EntryVwapAnchor) / state.EntryVwapAnchor) * 100
		return math.Abs(rawAnchorDistancePct / state.Profile.ADRPct)
	}
	return math.Abs(state.NormalizedVwapDistance)
}

func (e *Engine) appendAndPruneHistory(state *InstrumentState, bar *models.Bar) {
	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	barCutoff := bar.Timestamp.Add(-e.MaxBarLookback)
	validIdx := 0
	for i, historicalBar := range state.BarHistory[tf] {
		if historicalBar.Timestamp.Before(barCutoff) {
			validIdx = i + 1
		} else {
			break
		}
	}
	if validIdx > 0 {
		state.BarHistory[tf] = state.BarHistory[tf][validIdx:]
	}
}

// =========================================================================
// 🎯 TICK-LEVEL ROUTING UTILITIES (`UpdateContext`)
// =========================================================================

func (e *Engine) evaluateFlatTickEntry(state *InstrumentState, adrMultiplier float64) string {
	state.CurrentSetupPhase = PhaseNeutral
	state.PeakVwapExtension = 0.0

	setupSignal := e.ActiveStrategy.CheckEntry(state)
	absAdrDistance := math.Abs(state.NormalizedVwapDistance)

	if (setupSignal == "SETUP_READY_LONG" || setupSignal == "SETUP_READY_SHORT") && absAdrDistance <= adrMultiplier {
		e.mu.Unlock()
		if setupSignal == "SETUP_READY_LONG" {
			return "GO_LONG"
		}
		return "GO_SHORT"
	}

	if setupSignal == "GO_LONG" || setupSignal == "GO_SHORT" {
		e.mu.Unlock()
		return setupSignal
	}

	e.mu.Unlock()
	return "HOLD"
}

func (e *Engine) evaluateActiveTickPosition(state *InstrumentState, symbol, currentSide string, averagePrice float64, netQty int, adrMultiplier float64) string {
	state.CurrentSetupPhase = PhaseActiveTrade

	currentExtension := e.calculateCurrentExtension(state)
	if currentExtension > state.PeakVwapExtension {
		state.PeakVwapExtension = currentExtension
	}

	// Invalidation Protections
	maxAllowedCrossDistance := adrMultiplier * 2.0
	if currentSide == "LONG" && state.LatestPrice < state.LiveSessionVWAP && math.Abs(state.NormalizedVwapDistance) > maxAllowedCrossDistance {
		state.EntryVwapAnchor = 0.0
		e.mu.Unlock()
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.LatestPrice > state.LiveSessionVWAP && math.Abs(state.NormalizedVwapDistance) > maxAllowedCrossDistance {
		state.EntryVwapAnchor = 0.0
		e.mu.Unlock()
		return "EXIT_SHORT"
	}

	// Active Logging Trailing Check
	if openTrade, trackingTrade := e.ActiveTrades[symbol]; trackingTrade {
		if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
			state.EntryVwapAnchor = 0.0
			e.mu.Unlock()
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, state.LastUpdated)
			return "EXIT_" + currentSide
		}
	}

	e.mu.Unlock()
	return "HOLD"
}

// =========================================================================
// 🎯 BAR-SIGNAL ROUTING UTILITIES (`GenerateSignal`)
// =========================================================================

func (e *Engine) updateSignalPhaseAndExtensions(state *InstrumentState, currentSide string, averagePrice float64, netQty int) {
	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
		state.PeakVwapExtension = 0.0
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade

		currentExtension := math.Abs(state.NormalizedVwapDistance)
		if currentExtension > state.PeakVwapExtension {
			state.PeakVwapExtension = currentExtension
		}
		e.updateActiveTradePnL(state.Symbol, state.LatestPrice, averagePrice, netQty)
	}
}

func (e *Engine) updateActiveTradePnL(symbol string, latestPrice, averagePrice float64, netQty int) {
	if openTrade, trackingTrade := e.ActiveTrades[symbol]; trackingTrade {
		multiplier := 1.0
		if openTrade.TradeSide == "SHORT" {
			multiplier = -1.0
		}
		currentCashPnL := (latestPrice - averagePrice) * float64(netQty) * multiplier
		if currentCashPnL > openTrade.PeakPnLINR {
			openTrade.PeakPnLINR = currentCashPnL
		}
	}
}

func (e *Engine) processNeutralSignalRoute(symbol string, state *InstrumentState) string {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	signal := e.ActiveStrategy.CheckEntry(state)

	if signal == "GO_LONG" || signal == "GO_SHORT" {
		e.mu.Lock()
		e.initializeActiveTradeLog(symbol, state, signal)
		e.mu.Unlock()
		return signal
	}
	return "HOLD"
}

func (e *Engine) processActiveSignalRoute(symbol string, state *InstrumentState, currentSide string, averagePrice float64, netQty int) string {
	e.mu.RLock()
	openTrade, trackingTrade := e.ActiveTrades[symbol]
	e.mu.RUnlock()

	marketExitTime := state.LastUpdated

	if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
		if trackingTrade {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}

	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		if trackingTrade {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "STOP_LOSS", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}

	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		if trackingTrade {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "TAKE_PROFIT", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}

	signal := e.ActiveStrategy.CheckExit(state, currentSide)
	if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
		if trackingTrade {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "DIRECTION_FLIP", averagePrice, netQty, marketExitTime)
		}
		return signal
	}

	return "HOLD"
}

func (e *Engine) initializeActiveTradeLog(symbol string, state *InstrumentState, signal string) {
	tradeSide := "LONG"
	if signal == "GO_SHORT" {
		tradeSide = "SHORT"
	}

	if len(state.BarHistory["1m"]) > 0 {
		latestBar := state.BarHistory["1m"][len(state.BarHistory["1m"])-1]
		state.LastTradedBarTime = latestBar.Timestamp
	}

	state.EntryVwapAnchor = state.LiveSessionVWAP

	e.ActiveTrades[symbol] = &OptimizationTradeLog{
		Symbol:            symbol,
		StrategyName:      e.ActiveStrategy.Name(),
		TradeSide:         tradeSide,
		MinutesSinceOpen:  state.ConsecutiveClosesAboveVwap + state.ConsecutiveClosesBelowVwap,
		EntryTimestamp:    state.LastUpdated,
		EntryPrice:        state.LatestPrice,
		EntryVwap:         state.LiveSessionVWAP,
		EntryVolumeRank:   state.LatestVolumeRank,
		EntryPriceRank:    state.LatestPriceRank,
		EntryVwapDistance: state.NormalizedVwapDistance,
	}
}

func (e *Engine) dispatchCompleteLog(symbol string, trade *OptimizationTradeLog, exitPrice float64, reason string, avgPrice float64, qty int, exitTime time.Time) {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	trade.ExitTimestamp = exitTime
	trade.ExitPrice = exitPrice
	trade.ExitReason = reason

	multiplier := 1.0
	if trade.TradeSide == "SHORT" {
		multiplier = -1.0
	}

	trade.FinalPnLINR = (exitPrice - avgPrice) * float64(qty) * multiplier

	if e.OnTradeCompleted != nil {
		go e.OnTradeCompleted(trade)
	}
}
