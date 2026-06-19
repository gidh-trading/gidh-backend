package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
	"time"
)

const (
	// Entry Window
	StartTradingTime = 925 // 09:25
	EndTradingTime   = 950 // 09:50

	// Entry Thresholds
	MinVolumeRank    = 7.0
	MaxPriceRank     = 6.0
	MinEfficiency    = 25.0
	MinSlope         = 8.0
	MinTimeAboveVWAP = 70.0
	MaxTimeAboveVWAP = 30.0

	// Risk Management
	HardStopLossINR    = -350.0
	InitialTakeProfit  = 500.0
	DecayRatePerMinute = 15.0
	MinTakeProfitFloor = 300.0

	// Exit/Trailing Stop Parameters
	FailureExitMinutes    = 6.0
	FailureExitProfit     = 50.0
	TrailingPeakThreshold = 200.0
	TrailingStopLoss      = 25.0
)

type VwapEfficiencyMomentumStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
}

func NewVwapEfficiencyMomentumStrategy(configs map[string]*models.OptimizedStrategyConfig) *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName: "Algorithmic_Absorption_Scalp_Optimized",
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

func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.Warnf("cannot load time location: %v", err)
		loc = time.UTC
	}

	tf := "1m"
	history, exists := state.BarHistory[tf]

	if !exists || len(history) < 2 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	prevBar := history[len(history)-2]

	istTime := latestBar.Timestamp.In(loc)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()

	if currentHM < StartTradingTime || currentHM > EndTradingTime {
		return "HOLD"
	}

	volRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank
	efficiency := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope
	timePctVwap := latestBar.Analytics.TimePctAboveVwap

	if volRank < MinVolumeRank || priceRank > MaxPriceRank {
		return "HOLD"
	}

	// Bullish Absorption + Breakout
	if efficiency >= MinEfficiency && slope >= MinSlope && latestBar.Close > latestBar.VWAP && timePctVwap > MinTimeAboveVWAP {
		if latestBar.Close > prevBar.High {
			return "GO_LONG"
		}
	}

	// Bearish Absorption + Breakout
	if efficiency <= -MinEfficiency && slope <= -MinSlope && latestBar.Close < latestBar.VWAP && timePctVwap < MaxTimeAboveVWAP {
		if latestBar.Close < prevBar.Low {
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	if state.EntryTimestamp.IsZero() {
		return "HOLD"
	}

	minutesElapsed := state.LastTickTime.Sub(state.EntryTimestamp).Minutes()

	if minutesElapsed >= FailureExitMinutes && state.CurrentPnL < FailureExitProfit {
		return "EXIT_" + currentSide
	}

	return "HOLD"
}

func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfit
	}

	minutesElapsed := state.LastTickTime.Sub(state.EntryTimestamp).Minutes()
	dynamicTargetProfit := InitialTakeProfit - (minutesElapsed * DecayRatePerMinute)

	if dynamicTargetProfit < MinTakeProfitFloor {
		dynamicTargetProfit = MinTakeProfitFloor
	}

	return state.CurrentPnL >= dynamicTargetProfit
}

func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.PeakPnL >= TrailingPeakThreshold {
		if state.CurrentPnL <= TrailingStopLoss {
			return true
		}
	}

	return state.CurrentPnL <= HardStopLossINR
}
