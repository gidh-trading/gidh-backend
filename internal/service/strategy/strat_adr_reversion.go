package strategy

import (
	"gidh-backend/internal/service/models"
)

type StructuralReversionStrategy struct {
	cfg *Config
}

func NewStructuralReversionStrategy() *StructuralReversionStrategy {
	return &StructuralReversionStrategy{
		cfg: &Config{
			StartTradingTime:   915,  // 09:15 AM
			EndTradingTime:     1000, // 02:55 PM
			ForceExitTime:      1500, // 03:00 PM Auto Square-Off
			HardStopLossINR:    -1500.0,
			TakeProfitINR:      500.0,
			MaximumTradesCount: 1,
		},
	}
}

func (s *StructuralReversionStrategy) Name() string {
	return "Simple_ADR_Touch_Reversion"
}

func (s *StructuralReversionStrategy) Config() *Config {
	return s.cfg
}

// CheckEntry triggers directly on structural boundary touches
func (s *StructuralReversionStrategy) CheckEntry(state *InstrumentState) string {
	// 1. Fetch the historical bar slice
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	// 2. Extract the target execution bar (the most recent completed bar)
	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	adrHigh := latestBar.Analytics.ADRHigh
	adrLow := latestBar.Analytics.ADRLow

	// Safety check: Ensure structural ADR boundaries have actively initialized
	if adrHigh == 0 || adrLow == 0 {
		return "HOLD"
	}

	//if latestBar.High >= adrHigh {
	//	return "GO_SHORT"
	//}
	//
	//if latestBar.Low <= adrLow {
	//	return "GO_LONG"
	//}

	return "HOLD"
}

func (s *StructuralReversionStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD" // Handled globally via target TakeProfit/StopLoss parameters
}

func (s *StructuralReversionStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int, percentiles map[string]*models.VWAPDistancePercentile) bool {
	return state.CurrentPnL >= s.cfg.TakeProfitINR
}

func (s *StructuralReversionStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int, profiles map[string]*models.InstrumentProfile) bool {
	return state.CurrentPnL <= s.cfg.HardStopLossINR
}

func (s *StructuralReversionStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	state.ActiveStrategyName = s.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]StrategyStats)
	}

	stats := state.StrategyHistory[s.Name()]
	stats.IsCurrentlyActive = true
	state.StrategyHistory[s.Name()] = stats
}
