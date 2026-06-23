package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	StartTradingTime = 920
	EndTradingTime   = 950
	ExitTime         = 1015

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
	if state.StrategyHistory[s.Name()] {
		return "HOLD"
	}

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < StartTradingTime || currentTimeInt > EndTradingTime {
		return "HOLD"
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

	engineExitSignal := "EXIT_" + currentSide

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt > ExitTime {
		return engineExitSignal
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
	// Managed centrally by the TimeBasedRouter
}
