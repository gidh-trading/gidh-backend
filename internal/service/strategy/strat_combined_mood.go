package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	// Entry Time Optimization Window
	StartTradingTime = 920
	EndTradingTime   = 950
	ExitTime         = 1015

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
	if state.StrategyHistory[s.Name()] {
		return "HOLD"
	}

	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < StartTradingTime || currentTimeInt > EndTradingTime {
		return "HOLD"
	}

	// 1. Evaluate Long Entry Rules
	if s.evaluateLongEntry(latestBar) {
		return "GO_LONG"
	}

	// 2. Evaluate Short Entry Rules
	if s.evaluateShortEntry(latestBar) {
		return "GO_SHORT"
	}

	return "HOLD"
}

// evaluateLongEntry isolates the specific technical conditions for initiating a long position
func (s *CombinedMoodStrategy) evaluateLongEntry(latestBar *models.Bar) bool {
	combinedMood := latestBar.Analytics.NetVolumeMood + latestBar.Analytics.NetPriceMood
	volumeMood := latestBar.Analytics.NetVolumeMood
	priceMood := latestBar.Analytics.NetPriceMood
	volumeRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank
	vwapDistance := latestBar.Analytics.NormalizedVwapDistance
	vwapTime := latestBar.Analytics.TimePctAboveVwap

	isRankValid := volumeRank > 3 && volumeRank < 7 && priceRank > 3 && priceRank < 7
	validCombinedMood := combinedMood > 70.0 && combinedMood < 130.0 && volumeMood > priceMood
	isBullish := latestBar.Analytics.Direction == models.DirBullish || latestBar.Analytics.Direction == models.DirStrongBullish
	isVwapValid := vwapDistance < 0.2 && vwapDistance > 0.09 && vwapTime > 70

	return validCombinedMood && isBullish && isRankValid && isVwapValid
}

// evaluateShortEntry isolates the technical conditions for initiating a short position
// Note: Customize these metrics (e.g., DirBearish, moods, ranks) to fit your shorting criteria.
func (s *CombinedMoodStrategy) evaluateShortEntry(latestBar *models.Bar) bool {
	combinedMood := latestBar.Analytics.NetVolumeMood + latestBar.Analytics.NetPriceMood
	volumeMood := latestBar.Analytics.NetVolumeMood
	priceMood := latestBar.Analytics.NetPriceMood
	volumeRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank
	vwapDistance := latestBar.Analytics.NormalizedVwapDistance
	vwapTime := latestBar.Analytics.TimePctAboveVwap

	// Place matching short-side structural checks here:
	isRankValid := volumeRank > 3 && volumeRank < 7 && priceRank > 3 && priceRank < 7
	validCombinedMood := combinedMood < -70.0 && combinedMood > -130.0 && volumeMood < priceMood
	isBearish := latestBar.Analytics.Direction == models.DirBearish || latestBar.Analytics.Direction == models.DirStrongBearish
	isVwapValid := vwapDistance > -0.2 && vwapDistance < 0.09 && vwapTime < 30

	return validCombinedMood && isBearish && isRankValid && isVwapValid
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

	// Determine the precise exit signal string the engine expects ("EXIT_LONG" or "EXIT_SHORT")
	engineExitSignal := "EXIT_" + currentSide

	// 1. Time-based Cutoff Evaluation
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
	// Left empty intentionally: Strategy tracking history is now isolated and
	// managed centrally by the TimeBasedRouter inside state.StrategyHistory
}
