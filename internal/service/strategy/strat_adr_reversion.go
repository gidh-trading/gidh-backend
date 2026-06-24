package strategy

import (
	"sync"
)

const (
	ADRRevStartTradingTime = 935  // 10:00 AM
	ADRRevEndTradingTime   = 1445 // 2:45 PM
	ADRRevForceExitTime    = 1455 // 2:55 PM (Right before 3:00 PM auto square-off)

	ADRRevHardStopLossINR = -551.0
	ADRRevTakeProfitINR   = 1000.0
)

type ADRPercentileReversionStrategy struct {
	strategyName string
	mu           sync.RWMutex
}

func NewADRPercentileReversionStrategy() *ADRPercentileReversionStrategy {
	return &ADRPercentileReversionStrategy{
		strategyName: "ADR_Percentile_Reversion",
	}
}

func (s *ADRPercentileReversionStrategy) Name() string {
	return s.strategyName
}

func (s *ADRPercentileReversionStrategy) CheckEntry(state *InstrumentState) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. Guard against empty context history or pre-calculated parameters
	tf := "1m"
	history, ok := state.BarHistory[tf]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil || state.VwapPercentile == nil || state.ADRHigh == 0 || state.ADRLow == 0 {
		return "HOLD"
	}

	// 2. Enforce one execution instance per stock per day for safety
	if state.StrategyHistory[s.Name()] {
		return "HOLD"
	}

	// 3. Time window checking
	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < ADRRevStartTradingTime || currentTimeInt > ADRRevEndTradingTime {
		return "HOLD"
	}

	// Extract pre-calculated pipeline properties
	vwapDistancePct := latestBar.Analytics.NormalizedVwapDistance

	// 🌟 Extract raw compounding directional momentum explicitly
	directionalMomentum := latestBar.Analytics.ContinuousPriceNormalized
	tickRank := latestBar.Analytics.TickRank

	// 4. Short Condition:
	// - Price overextended above VWAP and ADR High
	// - AND upside trend momentum is under control (e.g., below a threshold like 4.0)
	//   This blocks shorting directly into a vertical runaway short-squeeze.
	if vwapDistancePct > 0.5 && state.LatestPrice > state.ADRHigh && directionalMomentum < 15.0 && tickRank < 4 {
		return "GO_SHORT"
	}

	// 5. Long Condition:
	// - Price overextended below VWAP and ADR Low
	// - AND absolute downside momentum is under control (e.g., selling pressure isn't worse than -4.0)
	//   This blocks buying directly into a capitulation waterfall / falling knife.
	if vwapDistancePct < -0.5 && state.LatestPrice < state.ADRLow && directionalMomentum > -15.0 && tickRank < 4 {
		return "GO_LONG"
	}

	return "HOLD"
}

func (s *ADRPercentileReversionStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tf := "1m"
	history, ok := state.BarHistory[tf]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// 1. Enforce strict late afternoon square-off exit signal
	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt >= ADRRevForceExitTime {
		return "EXIT_" + currentSide // e.g., "EXIT_LONG" or "EXIT_SHORT"
	}

	// 2. Structural Profit Reversion target: Exit when price regains VWAP equilibrium
	if currentSide == "LONG" && state.LatestPrice >= state.LiveSessionVWAP {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.LatestPrice <= state.LiveSessionVWAP {
		return "EXIT_SHORT"
	}

	return "HOLD"
}

func (s *ADRPercentileReversionStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL >= ADRRevTakeProfitINR
}

func (s *ADRPercentileReversionStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= ADRRevHardStopLossINR
}

func (s *ADRPercentileReversionStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	// Handled directly via top-level TimeBasedRouter
}
