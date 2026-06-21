package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	// Entry Time Optimization Window
	StartTradingTime = 920
	EndTradingTime   = 955

	// Risk Engine Parameters
	HardStopLossINR = -900.0
	TakeProfitINR   = 500.0
)

type CombinedMoodStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
	tradedStocks map[string]bool
}

func NewCombinedMoodStrategy(configs map[string]*models.OptimizedStrategyConfig) *CombinedMoodStrategy {
	return &CombinedMoodStrategy{
		strategyName: "Combined_Mood_Velocity_Direct",
		configs:      configs,
		tradedStocks: make(map[string]bool),
	}
}

func (s *CombinedMoodStrategy) Name() string {
	return s.strategyName
}

func (s *CombinedMoodStrategy) CheckEntry(state *InstrumentState) string {
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

	// Avoid re-trading the same stock if already tracked
	if s.tradedStocks[latestBar.StockName] {
		return "HOLD"
	}

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < StartTradingTime || currentTimeInt > EndTradingTime {
		return "HOLD"
	}

	// Direct rules evaluation
	combinedMood := latestBar.Analytics.NetVolumeMood + latestBar.Analytics.NetPriceMood

	validCombinedMood := combinedMood > 70.0 && combinedMood < 100.0
	isBullish := latestBar.Analytics.Direction == models.DirBullish || latestBar.Analytics.Direction == models.DirStrongBullish

	if validCombinedMood && isBullish {
		return "GO_LONG"
	}

	return "HOLD"
}

func (s *CombinedMoodStrategy) CheckExit(state *InstrumentState, currentSide string) string {
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

	// Only process our target direction
	if currentSide == "LONG" {
		combinedMood := latestBar.Analytics.NetVolumeMood + latestBar.Analytics.NetPriceMood

		// Condition: Exit when combined mood breaks over 140
		if combinedMood > 140 {
			return "EXIT"
		}
	}

	return "HOLD"
}

func (s *CombinedMoodStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL >= TakeProfitINR
}

func (s *CombinedMoodStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= HardStopLossINR
}

func (s *CombinedMoodStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tradedStocks[symbol] = true
}
