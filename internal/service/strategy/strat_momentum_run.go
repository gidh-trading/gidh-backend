package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	MomentumStartTradingTime = 920
	MomentumEndTradingTime   = 955  // Expanded window to catch mid-day momentum breakouts
	MomentumExitTime         = 1015 // Final intraday square-off time

	MomentumHardStopLossINR = -900.0 // Adjusted for high-velocity runs
	MomentumTakeProfitINR   = 600.0  // Extended target to ride out full momentum trends
)

type MomentumRunStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
	tradedStocks map[string]bool
}

func NewMomentumRunStrategy(configs map[string]*models.OptimizedStrategyConfig) *MomentumRunStrategy {
	return &MomentumRunStrategy{
		strategyName: "Momentum_Run_Continuous",
		configs:      configs,
		tradedStocks: make(map[string]bool),
	}
}

func (s *MomentumRunStrategy) Name() string {
	return s.strategyName
}

func (s *MomentumRunStrategy) CheckEntry(state *InstrumentState) string {
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

	if state.StrategyHistory[s.Name()] {
		return "HOLD"
	}

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < MomentumStartTradingTime || currentTimeInt > MomentumEndTradingTime {
		return "HOLD"
	}

	volIntensity := latestBar.Analytics.ContinuousVolumeIntensity
	priceNorm := latestBar.Analytics.ContinuousPriceNormalized

	// 🎯 Volume energy sweet-spot
	if volIntensity > 3.0 && volIntensity < 5.0 {

		// 🎯 Corrected: Return GO_LONG / GO_SHORT to match the Engine expectations
		if priceNorm > 1.5 {
			return "GO_LONG"
		}

		if priceNorm < -1.5 {
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

func (s *MomentumRunStrategy) CheckExit(state *InstrumentState, currentSide string) string {
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

	engineExitSignal := "EXIT_" + currentSide // Results in "EXIT_LONG" or "EXIT_SHORT"

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt > MomentumExitTime {
		return engineExitSignal
	}

	priceNorm := latestBar.Analytics.ContinuousPriceNormalized

	// 🎯 Corrected: Match against "LONG" and "SHORT" as defined by the RiskManager
	if currentSide == "LONG" && priceNorm < 0.0 {
		return engineExitSignal
	}
	if currentSide == "SHORT" && priceNorm > 0.0 {
		return engineExitSignal
	}

	return "HOLD"
}

func (s *MomentumRunStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL >= MomentumTakeProfitINR
}

func (s *MomentumRunStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= MomentumHardStopLossINR
}

func (s *MomentumRunStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	// Managed centrally by the TimeBasedRouter
}
