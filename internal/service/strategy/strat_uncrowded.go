package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
)

const (
	// Entry Time Optimization Window
	StartTradingTime = 925
	EndTradingTime   = 1030

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
	var bar, tMinusOneBar, tMinusTwoBar *models.Bar
	if history, ok := state.BarHistory[tf]; ok && len(history) > 2 {
		bar = history[len(history)-1]
		tMinusOneBar = history[len(history)-2]
		tMinusTwoBar = history[len(history)-3]
	}

	if bar == nil || tMinusOneBar == nil || tMinusTwoBar == nil {
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

	// -------------------------------------------------------------
	// THE DECOUPLED "UNCROWDED" ALGO LOGIC RULES
	// -------------------------------------------------------------

	// Rule A: Dynamic Stealth Volume Check
	// Volume mood must show active steady buildup but can't be at crowded panic levels
	volumeCommitmentValid := bar.Analytics.NetVolumeMood >= 15.0 && bar.Analytics.NetVolumeMood <= 50.0

	// Rule B: Pure Low-Friction Price Expansion Velocity
	// Because it's not crowded, price should be highly efficient, displacing cleanly upwards
	priceVelocityValid := bar.Analytics.NetPriceMood >= 30.0 && bar.Analytics.NetPriceMood <= 70.0

	// Rule C: The "Path of Least Resistance" Divergence Filter
	// In an uncrowded breakout, Price Efficiency MUST outpace Volume Commitment.
	// If VolumeMood > PriceMood, it means heavy crowded churning/absorption—We DO NOT buy that.
	isLowFrictionIgnition := bar.Analytics.NetPriceMood > bar.Analytics.NetVolumeMood

	// Rule D: Anchored Structural Support Floor
	// Price must be cleanly drifting and holding above the volume-weighted baseline
	vwapValid := bar.Close > bar.VWAP &&
		bar.Analytics.NormalizedVwapDistance < 0.20 &&
		tMinusOneBar.Close > tMinusOneBar.VWAP

	// Rule E: Order Flow Execution State
	// Sideways or absorption states are completely filtered out. We want standard, clean progression.
	directionValid := bar.Analytics.Direction == models.DirBullish

	// Rule F: Hard Rank Filters (Enforces your precise 4-5 uncrowded target zone)
	rankValid := (bar.Analytics.VolumeRank >= MinVolumeRank && bar.Analytics.VolumeRank <= MaxVolumeRank) &&
		(bar.Analytics.PriceRank >= MinPriceRank && bar.Analytics.PriceRank <= MaxPriceRank)

	// Trigger Long scalp entry when the uncrowded parameters hit perfectly
	if volumeCommitmentValid && priceVelocityValid && isLowFrictionIgnition && vwapValid && directionValid && rankValid {
		logger.Infof("[UNCROWDED-ENTRY] Stealth Long Triggered for %s. VolRank: %d, PriceRank: %d | NetPriceMood: %.2f, NetVolumeMood: %.2f (Friction Delta: %.2f)",
			bar.StockName,
			bar.Analytics.VolumeRank,
			bar.Analytics.PriceRank,
			bar.Analytics.NetPriceMood,
			bar.Analytics.NetVolumeMood,
			bar.Analytics.NetPriceMood-bar.Analytics.NetVolumeMood,
		)
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
