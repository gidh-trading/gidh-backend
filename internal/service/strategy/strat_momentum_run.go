package strategy

import (
	"gidh-backend/internal/service/models"
)

type MomentumRunStrategy struct {
	cfg *Config
}

func NewMomentumRunStrategy() *MomentumRunStrategy {
	return &MomentumRunStrategy{
		cfg: &Config{
			StartTradingTime:   915,  // 09:15 AM
			EndTradingTime:     1500, // 03:00 PM
			ForceExitTime:      1515, // 03:15 PM Auto Square-Off
			HardStopLossINR:    -400.0,
			TakeProfitINR:      2000.0, // High ceiling since we use TSL to exit
			MaximumTradesCount: 1,      // Max trades per stock today

			// Dynamic Trailing Stop Loss Configuration
			TrailActivationINR: 600.0,
			TrailCallbackINR:   250.0,
		},
	}
}

func (s *MomentumRunStrategy) Name() string {
	return "Momentum_Run_Flow_Thrust"
}

func (s *MomentumRunStrategy) Config() *Config {
	return s.cfg
}

// CheckEntry monitors order flow expansion vectors on the last closed bar
func (s *MomentumRunStrategy) CheckEntry(state *InstrumentState) string {
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	return "HOLD"
}

func (s *MomentumRunStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD" // Handled structural-wide through target CheckStopLoss / CheckTakeProfit
}

func (s *MomentumRunStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int, percentiles map[string]*models.VWAPDistancePercentile) bool {
	return state.CurrentPnL >= s.cfg.TakeProfitINR
}

// CheckStopLoss tracks initial protection alongside our absolute INR Trailing Stop Loss
func (s *MomentumRunStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int, profiles map[string]*models.InstrumentProfile) bool {
	// 1. Core Initial Hard Stop Protection
	if state.CurrentPnL <= s.cfg.HardStopLossINR {
		return true
	}

	// 2. Tiered Anti-Bleed Profit Lock Mechanism (Prevents Leaking Money)
	switch {
	case state.PeakPnL >= 1500.0:
		// Tier 3: Extreme thrust reached. Tighten the floor aggressively.
		trailingStopFloor := state.PeakPnL - 300.0
		if state.CurrentPnL < trailingStopFloor {
			return true
		}

	case state.PeakPnL >= 1000.0:
		// Tier 2: Solid expansion. Lock in at least half (+600 INR)
		if state.CurrentPnL < 600.0 {
			return true
		}

	case state.PeakPnL >= 600.0:
		// Tier 1: Break-even milestone. Protect capital + friction costs
		if state.CurrentPnL < 100.0 {
			return true
		}
	}

	return false
}

func (s *MomentumRunStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	state.ActiveStrategyName = s.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]StrategyStats)
	}

	stats := state.StrategyHistory[s.Name()]
	stats.IsCurrentlyActive = true
	state.StrategyHistory[s.Name()] = stats
}
