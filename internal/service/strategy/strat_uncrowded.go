package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	// Entry Time Optimization Window
	StartTradingTime = 918
	EndTradingTime   = 1005

	// 🛑 THE UNCROWDED FILTER ENCLOSURE
	// We precisely isolate the stealth ignition zone (P50 to P75)
	MinVolumeRank = 4
	MaxVolumeRank = 5

	// Price must show real, validated structural displacement (P50 to P90)
	MinPriceRank = 4
	MaxPriceRank = 6

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

	volumeCommitmentValid := bar.Analytics.NetVolumeMood > 30.0 && bar.Analytics.NetVolumeMood < 80.0

	vwapValid := bar.Analytics.NormalizedVwapDistance > 0.05 &&
		bar.Analytics.NormalizedVwapDistance < 0.2 &&
		bar.Analytics.TimePctAboveVwap > 90.0

	directionValid := bar.Analytics.Direction == models.DirBullish || bar.Analytics.Direction == models.DirStrongBullish

	if volumeCommitmentValid && vwapValid && directionValid {
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
