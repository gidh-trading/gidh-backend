package strategy

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
	"sync"
	"time"
)

const (
	// Entry Window
	StartTradingTime = 920  // 09:25
	EndTradingTime   = 1030 // 09:50

	// Entry Thresholds
	MinRank = 5.0 // Updated threshold

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
	return &VwapEfficiencyMomentumStrategy{
		strategyName: "Algorithmic_Absorption_Scalp_Optimized",
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
	if s.tradedStocks[state.StockName] {
		s.mu.RUnlock()
		return "HOLD"
	}
	s.mu.RUnlock()

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

	prevBarValid := prevBar.Analytics.VolumeRank >= MinRank &&
		prevBar.Analytics.PriceRank >= MinRank &&
		prevBar.Close > prevBar.VWAP &&
		(prevBar.Analytics.Direction == models.DirBullish || prevBar.Analytics.Direction == models.DirStrongBullish) &&
		prevBar.Analytics.NetEfficiency > 20

	latestBarValid := latestBar.Analytics.VolumeRank >= MinRank &&
		latestBar.Analytics.PriceRank >= MinRank &&
		latestBar.Close > latestBar.VWAP &&
		(latestBar.Analytics.Direction == models.DirBullish || latestBar.Analytics.Direction == models.DirStrongBullish) &&
		prevBar.Analytics.NetEfficiency > 20 &&
		latestBar.Analytics.NetEfficiencySlope > prevBar.Analytics.NetEfficiencySlope

	if prevBarValid && latestBarValid && latestBar.Analytics.NetEfficiencySlope > 5 {
		return "GO_LONG"
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

	// 🟢 Calculate elapsed minutes after the initial breathing space/grace period
	minutesToDecay := minutesElapsed - TakeProfitGraceMins
	if minutesToDecay < 0 {
		minutesToDecay = 0
	}

	// 🟢 Calculate decay directly against InitialTakeProfitINR
	decayAmount := minutesToDecay * DecayRatePerMinute
	decayedTarget := InitialTakeProfitINR - decayAmount

	// Check if current PnL has cleared the decayed benchmark
	if state.CurrentPnL >= decayedTarget {
		return true
	}

	return false
}

func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {

	if state.CurrentPnL <= HardStopLossINR {
		return true
	}

	return false
}

func (s *VwapEfficiencyMomentumStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	s.mu.Lock()
	s.tradedStocks[symbol] = true
	s.mu.Unlock()
	logger.Infof("Strategy [%s] marked stock %s as traded for the session.", s.strategyName, symbol)
}
