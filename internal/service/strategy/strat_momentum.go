package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
	"time"
)

const (
	InitialTakeProfitINR = 500.0
	DecayRatePerMinute   = 15.0
	MinTakeProfitFloor   = 300.0
	HardStopLossINR      = -350.0
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
	timePctVwap := latestBar.Analytics.TimePctAboveVwap

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
	if efficiency > 20.0 && slope > 5.0 && latestBar.Close > latestBar.VWAP && timePctVwap > 60.0 {
		return "GO_LONG"
	}

	// BEARISH ABSORPTION:
	// Sellers are passively absorbing buyers. Efficiency is highly negative despite price not dropping yet.
	if efficiency < -20.0 && slope < -5.0 && latestBar.Close < latestBar.VWAP && timePctVwap < 40.0 {
		return "GO_SHORT"
	}

	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	if state.EntryTimestamp.IsZero() {
		return "HOLD"
	}

	marketTime := state.LastTickTime
	minutesElapsed := marketTime.Sub(state.EntryTimestamp).Minutes()

	// If the trade hasn't moved into profit after 8 minutes, the absorption failed. Scratch the trade.
	if minutesElapsed >= 8.0 && state.CurrentPnL < 100.0 {
		return "EXIT_" + currentSide
	}

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

func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// 1. BREAKEVEN STOP LOSS: Protect profits
	// If the trade went into solid profit (> 200 INR), move stop loss to breakeven to cover taxes/brokerage
	if state.PeakPnL >= 200.0 {
		// Assume ~150 INR covers brokerage/slippage
		if state.CurrentPnL <= -150.0 {
			return true // Exit before the massive -400 drop
		}
	}

	// 2. THE MONETARY GUILLOTINE (Fallback)
	if state.CurrentPnL <= HardStopLossINR {
		return true
	}

	return false
}
