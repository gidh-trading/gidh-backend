package strategy

import (
	"sync"
	"time"
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
	s.mu.RLock()
	defer s.mu.RUnlock()

	// 1. Ensure the percentile baseline for this symbol is present
	if state.VwapPercentile == nil {
		return "HOLD"
	}

	// 2. Fetch the latest closed 1-minute bar from the history cache
	tf := "1m"
	history, ok := state.BarHistory[tf]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// Avoid re-trading the same asset if it has already triggered today
	if state.StrategyHistory[s.Name()] {
		return "HOLD"
	}

	// 3. Validate market operational hours context boundary
	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt < ReversionStartTradingTime || currentTimeInt > ReversionEndTradingTime {
		return "HOLD"
	}

	// 4. Extract the closed bar's normalized distance metric
	vwapDistance := latestBar.Analytics.NormalizedVwapDistance
	positiveMaxDistance := state.VwapPercentile.PosP99
	negativeMaxDistance := -1 * state.VwapPercentile.NegP99

	// 5. Mean-Reversion Evaluation Framework
	if vwapDistance > 0 {
		// Price has over-extended above VWAP.
		// If normalized distance breaches the positive P90 threshold, enter SHORT.
		if vwapDistance >= positiveMaxDistance {
			return "GO_SHORT"
		}
	} else if vwapDistance < 0 {
		if vwapDistance <= negativeMaxDistance {
			return "GO_LONG"
		}
	}

	return "HOLD"
}

func (s *VWAPPercentileReversionStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "1m"
	history, ok := state.BarHistory[tf]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// Time-based execution safety cutoff
	t := latestBar.Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()
	if currentTimeInt > ReversionExitTime {
		return "EXIT_" + currentSide
	}

	return "HOLD"
}

func (s *VWAPPercentileReversionStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// Starts at 600.0, subtracts 100.0 for every completed 30-minute interval, caps floor at 300.0
	return CheckTakeProfitWithIntervalDecay(state, ReversionTakeProfitINR, 100.0, 30*time.Minute, 300.0)
}

func (s *VWAPPercentileReversionStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return state.CurrentPnL <= ReversionHardStopLossINR
}

func (s *VWAPPercentileReversionStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	// Left empty intentionally: Strategy tracking history is now isolated and
	// managed centrally by the TimeBasedRouter inside state.StrategyHistory
}
