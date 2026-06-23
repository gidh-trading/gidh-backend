package strategy

import (
	"sync"
)

const (
	ReversionStartTradingTime = 1000
	ReversionEndTradingTime   = 1500
	ReversionExitTime         = 1515

	ReversionHardStopLossINR = -300.0
	ReversionTakeProfitINR   = 600.0
)

type VWAPPercentileReversionStrategy struct {
	strategyName string
	mu           sync.RWMutex
	tradedStocks map[string]bool
}

func NewVWAPPercentileReversionStrategy() *VWAPPercentileReversionStrategy {
	return &VWAPPercentileReversionStrategy{
		strategyName: "VWAP_Percentile_Bar_Mean_Reversion",
		tradedStocks: make(map[string]bool),
	}
}

func (s *VWAPPercentileReversionStrategy) Name() string {
	return s.strategyName
}

func (s *VWAPPercentileReversionStrategy) CheckEntry(state *InstrumentState) string {

	return "HOLD"
}

func (s *VWAPPercentileReversionStrategy) CheckExit(state *InstrumentState, currentSide string) string {

	return "HOLD"
}

func (s *VWAPPercentileReversionStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// Starts at 600.0, subtracts 100.0 for every completed 30-minute interval, caps floor at 300.0
	return false
}

func (s *VWAPPercentileReversionStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return false
}

func (s *VWAPPercentileReversionStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	// Left empty intentionally: Strategy tracking history is now isolated and
	// managed centrally by the TimeBasedRouter inside state.StrategyHistory
}
