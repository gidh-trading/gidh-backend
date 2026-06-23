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

	MinVolumeRank = 4
	MaxVolumeRank = 6
	MinPriceRank  = 4
	MaxPriceRank  = 7

	MinCombinedMood = 60.0
	MaxCombinedMood = 100.0

	MinVolumeMoodThreshold = 30.0
	MinPriceMoodThreshold  = 20.0

	LongVwapTimeMinPct  = 70.0
	ShortVwapTimeMaxPct = 30.0
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

// evaluateEntrySide unifies long and short setups into a single mathematical evaluation path
// evaluateEntrySide unifies long and short setups into a single mathematical evaluation path
func (s *CombinedMoodStrategy) evaluateEntrySide(latestBar *models.Bar, state *InstrumentState, sideSign float64) bool {
	analytics := latestBar.Analytics

	// 1. Clean, Standard Inclusive Range Check for Ranks
	if analytics.VolumeRank < MinVolumeRank || analytics.VolumeRank > MaxVolumeRank ||
		analytics.PriceRank < MinPriceRank || analytics.PriceRank > MaxPriceRank {
		return false
	}

	// 2. Directional & Temporal VWAP Alignment Filters
	if sideSign > 0 { // Long Side Specifics
		if analytics.Direction != models.DirBullish && analytics.Direction != models.DirStrongBullish {
			return false
		}
		if analytics.TimePctAboveVwap <= LongVwapTimeMinPct {
			return false
		}
	} else { // Short Side Specifics
		if analytics.Direction != models.DirBearish && analytics.Direction != models.DirStrongBearish {
			return false
		}
		if analytics.TimePctAboveVwap >= ShortVwapTimeMaxPct {
			return false
		}
	}

	// 3. Normalized Momentum Mood Matrix Checks (Flipped cleanly using sideSign)
	combinedMood := (analytics.NetVolumeMood + analytics.NetPriceMood) * sideSign
	volumeMood := analytics.NetVolumeMood * sideSign
	priceMood := analytics.NetPriceMood * sideSign

	if combinedMood <= MinCombinedMood || combinedMood >= MaxCombinedMood ||
		volumeMood <= MinVolumeMoodThreshold || priceMood <= MinPriceMoodThreshold {
		return false
	}

	// 4. Clean Absolute VWAP Target Band Isolation
	var p50, p75 float64
	if sideSign > 0 {
		p50 = state.VwapPercentile.PosP50
		p75 = state.VwapPercentile.PosP75
	} else {
		// Pulled directly as absolute positive numbers from your DB percentiles schema
		p50 = state.VwapPercentile.NegP50
		p75 = state.VwapPercentile.NegP75
	}

	// Transform distance into a pure positive magnitude matching your absolute database pool bounds
	// Long: positive * 1 = positive | Short: negative * -1 = positive
	distanceMagnitude := analytics.NormalizedVwapDistance * sideSign

	// Simple, clean, bulletproof inequality boundary verification
	return distanceMagnitude >= p50 && distanceMagnitude < p75
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
