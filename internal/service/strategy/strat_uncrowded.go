package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	// Entry Time Optimization Window
	StartTradingTime = 920
	EndTradingTime   = 1005

	// Risk Engine Parameters
	HardStopLossINR      = -400.0
	InitialTakeProfitINR = 500.0
	DecayRatePerMinute   = 15.0
	TakeProfitGraceMins  = 1.0
)

type UncrowdedEfficiencyStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
	tradedStocks map[string]bool
}

func NewUncrowdedEfficiencyStrategy(configs map[string]*models.OptimizedStrategyConfig) *UncrowdedEfficiencyStrategy {
	return &UncrowdedEfficiencyStrategy{
		strategyName: "Uncrowded_Stealth_Ignition_Scalp",
		configs:      configs,
		tradedStocks: make(map[string]bool),
	}
}

func (s *UncrowdedEfficiencyStrategy) Name() string {
	return s.strategyName
}

func (s *UncrowdedEfficiencyStrategy) CheckEntry(state *InstrumentState) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tf := "1m"
	var bar *models.Bar
	if history, ok := state.BarHistory[tf]; ok && len(history) > 2 {
		bar = history[len(history)-1]
	}

	if bar == nil {
		return "HOLD"
	}

	if s.tradedStocks[bar.StockName] {
		return "HOLD"
	}

	t := bar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < StartTradingTime || currentTimeInt > EndTradingTime {
		return "HOLD"
	}

	volumeCommitmentValid := bar.Analytics.NetVolumeMood > 25.0 && bar.Analytics.NetVolumeMood < 80.0
	priceMovementValid := bar.Analytics.NetPriceMood > 10.0 && bar.Analytics.NetPriceMood < 50.0

	vwapValid := bar.Analytics.NormalizedVwapDistance > 0.1 &&
		bar.Analytics.NormalizedVwapDistance < 0.2 && bar.Analytics.TimePctAboveVwap < 95

	directionValid := bar.Analytics.Direction == models.DirBullish || bar.Analytics.Direction == models.DirStrongBullish

	if volumeCommitmentValid && priceMovementValid && vwapValid && directionValid {
		return "GO_LONG"
	}

	return "HOLD"
}

func (s *UncrowdedEfficiencyStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}

func (s *UncrowdedEfficiencyStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfitINR
	}

	minutesElapsed := state.LastTickTime.Sub(state.EntryTimestamp).Minutes()
	minutesToDecay := minutesElapsed - TakeProfitGraceMins
	if minutesToDecay < 0 {
		minutesToDecay = 0
	}

	decayedTarget := InitialTakeProfitINR - (minutesToDecay * DecayRatePerMinute)
	return state.CurrentPnL >= decayedTarget
}

func (s *UncrowdedEfficiencyStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= HardStopLossINR
}

func (s *UncrowdedEfficiencyStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tradedStocks[symbol] = true
}
