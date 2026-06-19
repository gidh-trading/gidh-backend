package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
)

const (
	// Entry Window
	StartTradingTime = 925  // Adjusted to 09:25
	EndTradingTime   = 1030 // Extended to 15:00 or your custom session end
	MinRank          = 4
	MaxRank          = 5

	// Risk Management
	HardStopLossINR      = -400.0
	InitialTakeProfitINR = 500.0
	DecayRatePerMinute   = 15.0
	TakeProfitGraceMins  = 1.0
)

type VwapEfficiencyMomentumStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
	tradedStocks map[string]bool // Track stocks that have been traded
}

func NewVwapEfficiencyMomentumStrategy(configs map[string]*models.OptimizedStrategyConfig) *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{strategyName: "Algorithmic_Absorption_Scalp_Continuous",
		configs:      configs,
		tradedStocks: make(map[string]bool),
	}
}

func (s *VwapEfficiencyMomentumStrategy) Name() string {
	return s.strategyName
}

func (s *VwapEfficiencyMomentumStrategy) UpdateConfigs(newConfigs map[string]*models.OptimizedStrategyConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = newConfigs
}

func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	tf := "1m"
	// 1. Guard Condition: Extract the latest 1-minute bar from the Instrument State history
	var bar, tMinusOneBar, tMinusTwoBar *models.Bar
	if history, ok := state.BarHistory[tf]; ok && len(history) > 2 {
		bar = history[len(history)-1]
		tMinusOneBar = history[len(history)-2]
		tMinusTwoBar = history[len(history)-3]
	}

	// If no 1-minute data has been accumulated yet, safely skip evaluation
	if bar == nil {
		return "HOLD"
	}

	// 2. Prevent over-trading the same instrument in a session
	if s.tradedStocks[bar.StockName] {
		return "HOLD"
	}

	// 3. Session Chronological Time Filter (09:25 to 10:30)
	t := bar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < StartTradingTime || currentTimeInt > EndTradingTime {
		return "HOLD"
	}

	// 4. Multi-Dimensional Continuous State Filters

	// Rule A: High Directional Price Conviction (Clean Body Breakouts > 30%)
	priceConvictionValid := bar.Analytics.NetPriceEfficiency > 30.0 && bar.Analytics.NetPriceEfficiency < 60.0

	// Rule B: Buyer Volume Participation Confirmation (> 25% Institutional Backing)
	volumeParticipationValid := bar.Analytics.NetVolumeEfficiency > 25.0 && bar.Analytics.NetVolumeEfficiency < 70

	// Rule C: Overextension Protection (Prevents chasing top of massive runups)
	notOverextended := bar.Analytics.MeanReversionPressure < 70.0

	// Rule D: Structural Trend Floor (Price spent over 70% of the session above VWAP)
	vwapValid := bar.Close > bar.VWAP && bar.Analytics.NormalizedVwapDistance < 0.25 && tMinusOneBar.Close > tMinusOneBar.VWAP && tMinusTwoBar.Close > tMinusTwoBar.VWAP

	// Rule E: Direction check
	directionValid := bar.Analytics.Direction == models.DirBullish || bar.Analytics.Direction == models.DirStrongBullish

	volumeRankValid := bar.Analytics.VolumeRank >= MinRank && bar.Analytics.VolumeRank <= MaxRank
	priceRankValid := bar.Analytics.PriceRank >= MinRank && bar.Analytics.PriceRank <= MaxRank

	rankValid := volumeRankValid && priceRankValid

	// Trigger entry when vectors line up cleanly
	if priceConvictionValid && volumeParticipationValid && notOverextended && vwapValid && directionValid && rankValid {
		logger.Infof("[STRATEGY] GO_LONG breakout entry triggered for %s. PriceEff: %.2f, VolEff: %.2f, MeanRev: %.2f, AbsForce: %.2f, TimeVwapPct: %.2f",
			bar.StockName, bar.Analytics.NetPriceEfficiency, bar.Analytics.NetVolumeEfficiency,
			bar.Analytics.MeanReversionPressure, bar.Analytics.AbsorptionForce, bar.Analytics.TimePctAboveVwap)
		return "GO_LONG"
	}

	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// Let the Take Profit and Stop Loss managers govern core scalping exits
	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfitINR
	}

	marketTime := state.LastTickTime
	durationAlive := marketTime.Sub(state.EntryTimestamp)
	minutesElapsed := durationAlive.Minutes()

	// Calculate elapsed minutes after the grace breathing room window
	minutesToDecay := minutesElapsed - TakeProfitGraceMins
	if minutesToDecay < 0 {
		minutesToDecay = 0
	}

	// Linearly decay target to guarantee high-velocity capture turns over capital efficiently
	decayAmount := minutesToDecay * DecayRatePerMinute
	decayedTarget := InitialTakeProfitINR - decayAmount

	return state.CurrentPnL >= decayedTarget
}

func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= HardStopLossINR
}

func (s *VwapEfficiencyMomentumStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Mark symbol as traded for the session to prevent churning positions
	s.tradedStocks[symbol] = true
}
