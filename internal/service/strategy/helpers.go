package strategy

import (
	"gidh-backend/internal/service/models"
	"math"
	"time"
)

// =========================================================================
// ENGINE HELPER FUNCTIONS
// =========================================================================

func (e *Engine) getOrCreateState(symbol string) *InstrumentState {
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

func (e *Engine) evaluateLiveEntries(state *InstrumentState) string {
	state.CurrentSetupPhase = PhaseNeutral
	state.PeakVwapExtension = 0.0

	setupSignal := e.ActiveStrategy.CheckEntry(state)
	absAdrDistance := math.Abs(state.NormalizedVwapDistance)
	adrMultiplier := 0.05

	if setupSignal == "SETUP_READY_LONG" && absAdrDistance <= adrMultiplier {
		return "GO_LONG"
	}
	if setupSignal == "SETUP_READY_SHORT" && absAdrDistance <= adrMultiplier {
		return "GO_SHORT"
	}
	if setupSignal == "GO_LONG" || setupSignal == "GO_SHORT" {
		return setupSignal
	}
	return "HOLD"
}

func (e *Engine) evaluateLiveExits(state *InstrumentState, currentSide string, averagePrice float64, netQty int) string {
	state.CurrentSetupPhase = PhaseActiveTrade
	state.updateTrackExtension()

	// ⚡ Tick-Level Invalidation Wrong Side Crash Stop
	if state.isWrongSideCrashing(currentSide, 0.05*2.0) {
		state.EntryVwapAnchor = 0.0
		return "EXIT_" + currentSide
	}

	if openTrade, tracking := e.ActiveTrades[state.Symbol]; tracking {
		state.updateCashPeakPnL(openTrade, averagePrice, netQty)

		if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
			state.EntryVwapAnchor = 0.0
			e.dispatchCompleteLog(state.Symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, state.LastUpdated)
			return "EXIT_" + currentSide
		}
	}
	return "HOLD"
}

func (e *Engine) executeFlatDiscovery(symbol string, state *InstrumentState) string {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	signal := e.ActiveStrategy.CheckEntry(state)
	if signal == "GO_LONG" || signal == "GO_SHORT" {
		tradeSide := "LONG"
		if signal == "GO_SHORT" {
			tradeSide = "SHORT"
		}

		if len(state.BarHistory["1m"]) > 0 {
			state.LastTradedBarTime = state.BarHistory["1m"][len(state.BarHistory["1m"])-1].Timestamp
		}

		e.mu.Lock()
		state.EntryVwapAnchor = state.LiveSessionVWAP
		e.ActiveTrades[symbol] = &OptimizationTradeLog{
			Symbol:            symbol,
			StrategyName:      e.ActiveStrategy.Name(),
			TradeSide:         tradeSide,
			MinutesSinceOpen:  len(state.BarHistory["1m"]),
			EntryTimestamp:    state.LastUpdated,
			EntryPrice:        state.LatestPrice,
			EntryVwap:         state.LiveSessionVWAP,
			EntryVolumeRank:   state.LatestVolumeRank,
			EntryPriceRank:    state.LatestPriceRank,
			EntryVwapDistance: state.NormalizedVwapDistance,
		}
		e.mu.Unlock()
		return signal
	}
	return "HOLD"
}

func (e *Engine) executeActivePositionManagement(symbol string, state *InstrumentState, currentSide string, averagePrice float64, netQty int) string {
	e.mu.RLock()
	openTrade, tracking := e.ActiveTrades[symbol]
	e.mu.RUnlock()

	marketExitTime := state.LastUpdated

	if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
		if tracking {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}
	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		if tracking {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "STOP_LOSS", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}
	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		if tracking {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "TAKE_PROFIT", averagePrice, netQty, marketExitTime)
		}
		return "EXIT_" + currentSide
	}

	signal := e.ActiveStrategy.CheckExit(state, currentSide)
	if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
		if tracking {
			e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "DIRECTION_FLIP", averagePrice, netQty, marketExitTime)
		}
		return signal
	}

	return "HOLD"
}

func (e *Engine) dispatchCompleteLog(symbol string, trade *OptimizationTradeLog, exitPrice float64, reason string, avgPrice float64, qty int, exitTime time.Time) {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	state, exists := e.Registry[symbol]
	if exists {
		state.EntryVwapAnchor = 0.0
	}
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

// =========================================================================
// STATE HELPER FUNCTIONS
// =========================================================================

func (state *InstrumentState) updateBasicMetrics(bar *models.Bar) {
	state.LastUpdated = bar.Timestamp
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank

	if state.InitialOpenPrice == 0 && bar.Timestamp.Hour() == 9 && bar.Timestamp.Minute() >= 15 {
		state.InitialOpenPrice = bar.Open
	}
}

func (state *InstrumentState) isBeforeMarketOpen(t time.Time) bool {
	hm := (t.Hour() * 100) + t.Minute()
	return hm < 915
}

func (state *InstrumentState) trackVwapAcceptance(bar *models.Bar) {
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

func (state *InstrumentState) updateVolumeLedger(bar *models.Bar) {
	// Strictly check for elite Rank 7/7 footprint metrics
	if state.LatestVolumeRank == 7 && state.LatestPriceRank == 7 {
		switch bar.Analytics.Direction {
		case models.DirStrongBullish, models.DirBullish:
			state.BullishPushVolume += bar.Volume
		case models.DirStrongBearish, models.DirBearish:
			state.BearishPushVolume += bar.Volume
		}
	}
}

func (state *InstrumentState) calculateNormalizedDistance() {
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	} else {
		state.NormalizedVwapDistance = 0.0
	}
}

func (state *InstrumentState) appendAndPruneHistory(bar *models.Bar, maxLookback time.Duration) {
	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	barCutoff := bar.Timestamp.Add(-maxLookback)
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

func (state *InstrumentState) updateLiveTickData(enrichedTick *models.EnrichedTick) {
	rawTick := enrichedTick.Raw
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LiveSessionVWAP = rawTick.AverageTradedPrice

	if !state.HasInitializedGaps && (rawTick.Timestamp.Hour()*100+rawTick.Timestamp.Minute()) >= 915 {
		if rawTick.Change > 0.0 {
			state.IsGapUp = true
		} else if rawTick.Change < 0.0 {
			state.IsGapDown = true
		}
		state.HasInitializedGaps = true
	}

	state.calculateNormalizedDistance()
}

func (state *InstrumentState) updateTrackExtension() {
	var currentExtension float64
	if state.EntryVwapAnchor > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawAnchorDistancePct := ((state.LatestPrice - state.EntryVwapAnchor) / state.EntryVwapAnchor) * 100
		currentExtension = math.Abs(rawAnchorDistancePct / state.Profile.ADRPct)
	} else {
		currentExtension = math.Abs(state.NormalizedVwapDistance)
	}

	if currentExtension > state.PeakVwapExtension {
		state.PeakVwapExtension = currentExtension
	}
}

func (state *InstrumentState) isWrongSideCrashing(currentSide string, maxAllowedCrossDistance float64) bool {
	absDistance := math.Abs(state.NormalizedVwapDistance)
	if currentSide == "LONG" && state.LatestPrice < state.LiveSessionVWAP && absDistance > maxAllowedCrossDistance {
		return true
	}
	if currentSide == "SHORT" && state.LatestPrice > state.LiveSessionVWAP && absDistance > maxAllowedCrossDistance {
		return true
	}
	return false
}

func (state *InstrumentState) updateCashPeakPnL(openTrade *OptimizationTradeLog, averagePrice float64, netQty int) {
	multiplier := 1.0
	if openTrade.TradeSide == "SHORT" {
		multiplier = -1.0
	}
	currentCashPnL := (state.LatestPrice - averagePrice) * float64(netQty) * multiplier
	if currentCashPnL > openTrade.PeakPnLINR {
		openTrade.PeakPnLINR = currentCashPnL
	}
}
