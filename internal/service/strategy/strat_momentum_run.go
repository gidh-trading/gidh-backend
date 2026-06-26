package strategy

import "gidh-backend/internal/service/models"

type MomentumRunStrategy struct {
	cfg *Config
}

func NewMomentumRunStrategy() *MomentumRunStrategy {
	return &MomentumRunStrategy{
		cfg: &Config{
			StartTradingTime:   920,    // 09:15 AM
			EndTradingTime:     1030,   // 02:45 PM
			ForceExitTime:      1100,   // 03:00 PM Auto Square-Off
			HardStopLossINR:    -700.0, // Fixed Safety Risk Stop Floor
			TakeProfitINR:      700.0,  // Target Momentum Target Cap
			MaximumTradesCount: 1,      // Max Trades per stock limit rule
		},
	}
}

func (s *MomentumRunStrategy) Name() string {
	return "Momentum_Run"
}

func (s *MomentumRunStrategy) Config() *Config {
	return s.cfg
}

// updateAndGetScore updates a running momentum metric held inside the stock's unique metadata map
func (s *MomentumRunStrategy) updateAndGetScore(state *InstrumentState, latestBar *models.Bar) int {
	if state.Metadata == nil {
		state.Metadata = make(map[string]interface{})
	}

	// Initialize score if it doesn't exist
	currentScore := 0
	if val, ok := state.Metadata["momentum_score"]; ok {
		if scoreInt, ok := val.(int); ok {
			currentScore = scoreInt
		}
	}

	analytics := latestBar.Analytics

	// Momentum Scoring Evaluation Framework
	if analytics.RollingVolumeIntensity > 4.0 && analytics.RollingTickRank > 3.0 {
		if analytics.Direction == models.DirStrongBullish || analytics.Direction == models.DirBullish {
			currentScore++
		} else if analytics.Direction == models.DirStrongBearish || analytics.Direction == models.DirBearish {
			currentScore--
		}
	} else {
		// Decay momentum score slowly if transaction activity cools off
		if currentScore > 0 {
			currentScore--
		} else if currentScore < 0 {
			currentScore++
		}
	}

	state.Metadata["momentum_score"] = currentScore
	return currentScore
}

func (s *MomentumRunStrategy) CheckEntry(state *InstrumentState) string {
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// Calculate and store score on the isolated stock's state object
	score := s.updateAndGetScore(state, latestBar)

	// STRATEGY RULE: Enter when momentum run crosses 5
	if score >= 5 && latestBar.Close > latestBar.VWAP {
		return "GO_LONG"
	}

	if score <= -5 && latestBar.Close < latestBar.VWAP {
		return "GO_SHORT"
	}

	return "HOLD"
}

func (s *MomentumRunStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// Re-evaluate score sequence
	score := s.updateAndGetScore(state, latestBar)

	if currentSide == "LONG" {
		// STRATEGY RULE: Target Exit at 9 or 10 | Stop Loss drop at 3
		if score >= 9 || score <= 3 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		// Mirror target thresholds for shorts
		if score <= -9 || score >= -3 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *MomentumRunStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	// Integrates both score exit bands and hard currency ceilings
	return CheckTakeProfitWithDecay(state, s.cfg.TakeProfitINR, 10, 300)
}

func (s *MomentumRunStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return state.CurrentPnL <= s.cfg.HardStopLossINR
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
