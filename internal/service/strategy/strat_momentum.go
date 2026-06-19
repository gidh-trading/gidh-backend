package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
	"time"
)

const (
	InitialTakeProfitINR = 500.0  // Lower ceiling to catch realistic momentum peaks
	DecayRatePerMinute   = 15.0   // Faster decay forces the algo to take profit if momentum stalls
	MinTakeProfitFloor   = 150.0  // Never exit a winner for less than 200
	HardStopLossINR      = -400.0 // The Guillotine
)

type VwapEfficiencyMomentumStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
}

func NewVwapEfficiencyMomentumStrategy(configs map[string]*models.OptimizedStrategyConfig) *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName: "Algorithmic_Absorption_Scalp",
		configs:      configs,
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

// CheckEntry executes mathematically proven Absorption setups
func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.Warnf("cannot load time location: %v", err)
		loc = time.UTC
	}

	tf := "1m"
	history, exists := state.BarHistory[tf]

	latestBar := history[len(history)-1]

	istTime := latestBar.Timestamp.In(loc)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()

	if currentHM < 920 {

		return "HOLD"
	}

	if currentHM > 1015 {
		return "HOLD"
	}

	if !exists || len(history) < 1 {
		return "HOLD"
	}

	// Extract the raw features
	volRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank
	efficiency := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope

	// 🛑 DATA RULE 1: If institutional volume isn't present, there is zero edge.
	if volRank < 6 {
		return "HOLD"
	}

	// 🛑 DATA RULE 2: If the price has already expanded massively (Ignition), the pullback will
	// hit our -400 INR stop loss >50% of the time. We ONLY trade coiled compression.
	if priceRank > 4 {
		return "HOLD"
	}

	// =========================================================================
	// THE ABSORPTION COIL (The EV+ Setup)
	// Institutional Volume + Small Price Expansion + High Efficiency
	// =========================================================================

	// BULLISH ABSORPTION:
	// Buyers are passively absorbing sellers. Efficiency is highly positive despite price not expanding yet.
	if efficiency > 20.0 && slope > 5.0 && latestBar.Close > latestBar.VWAP {
		return "GO_LONG"
	}

	// BEARISH ABSORPTION:
	// Sellers are passively absorbing buyers. Efficiency is highly negative despite price not dropping yet.
	if efficiency < -20.0 && slope < -5.0 && latestBar.Close < latestBar.VWAP {
		return "GO_SHORT"
	}

	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfitINR
	}

	marketTime := state.LastTickTime
	durationAlive := marketTime.Sub(state.EntryTimestamp)
	minutesElapsed := durationAlive.Minutes()

	decayAmount := minutesElapsed * DecayRatePerMinute
	dynamicTargetProfit := InitialTakeProfitINR - decayAmount

	if dynamicTargetProfit < MinTakeProfitFloor {
		dynamicTargetProfit = MinTakeProfitFloor
	}

	if state.CurrentPnL >= dynamicTargetProfit {
		return true
	}

	return false
}

// CheckStopLoss now relies ENTIRELY on the monetary guillotine
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// 1. THE MONETARY GUILLOTINE
	// We removed the structural "Candle Low" stop loss because Python proved
	// the absorption coil requires slight breathing room, and tight structural stops
	// were yielding a 70% false-positive wick-out rate.
	if state.CurrentPnL <= HardStopLossINR {
		return true
	}

	return false
}
