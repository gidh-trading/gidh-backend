package strategy

import (
	"time"
)

type TimeBasedRouter struct {
	strategies map[string]Strategy
}

func NewTimeBasedRouter() *TimeBasedRouter {
	return &TimeBasedRouter{
		strategies: make(map[string]Strategy),
	}
}

func (r *TimeBasedRouter) Name() string { return "Dynamic_Multi_Strategy_Registry_Router" }

// RegisterStrategy permits registration of distinct strategies at boot-time with zero code modifications
func (r *TimeBasedRouter) RegisterStrategy(strat Strategy) {
	r.strategies[strat.Name()] = strat
}

// GetStrategies exposes all active running strategy configurations
func (r *TimeBasedRouter) GetStrategies() map[string]Strategy {
	return r.strategies
}

// ValidateTimeAndCooldowns audits trading eligibility using isolated strategy parameter rules
func (r *TimeBasedRouter) ValidateTimeAndCooldowns(strat Strategy, state *InstrumentState, marketTime time.Time, isFlat bool) (bool, bool) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		loc = time.UTC
	}

	istTime := marketTime.In(loc)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()
	cfg := strat.Config()

	// 1. Enforce Strategy-Level Maximum Trades Limit for this specific stock
	stats, exists := state.StrategyHistory[strat.Name()]
	if isFlat && exists && stats.TradeCount >= cfg.MaximumTradesCount {
		return false, false
	}

	// 2. Handle Strategy-Specific Forced Square-Off Boundaries
	if currentHM >= cfg.ForceExitTime {
		if !isFlat {
			return false, true // Signals shouldSquareOff = true to execute liquidation commands
		}
		return false, false
	}

	// 3. Enforce Strategy Active Trading Window
	if currentHM < cfg.StartTradingTime || currentHM > cfg.EndTradingTime {
		return false, false
	}

	// 4. Enforce Cooldown Breathing Space After Exit
	if isFlat && !state.LastExitSignalTime.IsZero() && marketTime.Sub(state.LastExitSignalTime) < 3*time.Minute {
		return false, false
	}

	return true, false
}
