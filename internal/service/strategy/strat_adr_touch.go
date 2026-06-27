package strategy

import "gidh-backend/internal/service/models"

type ADRReversalStrategy struct {
	cfg *Config
}

func NewADRReversalStrategy() *ADRReversalStrategy {
	return &ADRReversalStrategy{
		cfg: &Config{
			StartTradingTime:   915,    // 09:15 AM
			EndTradingTime:     1455,   // 02:55 PM
			ForceExitTime:      1500,   // 03:00 PM Auto Square-Off
			HardStopLossINR:    -700.0, // Disabled Safety Risk Floor
			TakeProfitINR:      1000.0, // Target Cap of 1000 RS
			MaximumTradesCount: 1,      // Max Trades per stock limit rule
		},
	}
}

func (s *ADRReversalStrategy) Name() string {
	return "ADR_High_Low_Reversal"
}

func (s *ADRReversalStrategy) Config() *Config {
	return s.cfg
}

func (s *ADRReversalStrategy) CheckEntry(state *InstrumentState) string {
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// 1. SHORT CONDITION: Price touches or breaks above the ADR High
	if latestBar.High >= state.ADRHigh {
		return "GO_SHORT"
	}

	// 2. LONG CONDITION: Price touches or falls below the ADR Low
	if latestBar.Low <= state.ADRLow {
		return "GO_LONG"
	}

	return "HOLD"
}

// CheckExit tracks structural mid-bar trend breaks.
// We return HOLD because exits are completely automated by TP and the 15:00 Time limits.
func (s *ADRReversalStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}

// CheckTakeProfit hooks up our automated configuration targets (1000 RS)
func (s *ADRReversalStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int, percentiles map[string]*models.VWAPDistancePercentile) bool {
	return state.CurrentPnL >= s.cfg.TakeProfitINR
}

// CheckStopLoss hooks up our automated safety risk floor limits (disabled via config floor)
func (s *ADRReversalStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int, profiles map[string]*models.InstrumentProfile) bool {
	return state.CurrentPnL <= s.cfg.HardStopLossINR
}

// OnEntryCommit modifies strategy-specific metadata states upon transaction entry confirmation
func (s *ADRReversalStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	state.ActiveStrategyName = s.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]StrategyStats)
	}

	stats := state.StrategyHistory[s.Name()]
	stats.IsCurrentlyActive = true
	state.StrategyHistory[s.Name()] = stats
}
