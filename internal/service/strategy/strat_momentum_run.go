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
			HardStopLossINR:    -850.0, // Fixed Safety Risk Stop Floor
			TakeProfitINR:      600.0,  // Target Momentum Target Cap
			MaximumTradesCount: 1,      // Max Trades per stock limit rule
		},
	}
}

func (s *MomentumRunStrategy) Name() string {
	return "Chapter_1_Momentum_Run"
}

func (s *MomentumRunStrategy) Config() *Config {
	return s.cfg
}

func (s *MomentumRunStrategy) CheckEntry(state *InstrumentState) string {
	history, ok := state.BarHistory["5m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	analytics := latestBar.Analytics

	// 1. Core Structural Momentum Baseline Filters
	// Volume effort and transaction velocity must represent clear active participation.
	hasVolumeEffort := analytics.RollingVolumeIntensity > 5.0
	hasTransactionVelocity := analytics.RollingTickRank > 3.0

	isBullish := latestBar.Analytics.Direction == models.DirBullish || latestBar.Analytics.Direction == models.DirStrongBullish
	isBearish := latestBar.Analytics.Direction == models.DirBearish || latestBar.Analytics.Direction == models.DirStrongBearish

	vwapDistance := latestBar.Analytics.NormalizedVwapDistance
	vwapClosePct := latestBar.Analytics.VwapClosePct

	// Proceed only if high execution pace and volume confirmation are met
	if !hasVolumeEffort || !hasTransactionVelocity {
		return "HOLD"
	}

	// 2. BULL RUN CONDITIONS (Go Long)
	// Price expansion is trending upwards and value sits safely above the anchor.
	isBullExpansion := analytics.RollingPriceNormalized > 3
	isAboveValue := latestBar.Close > latestBar.VWAP

	if isBullish && isBullExpansion && isAboveValue && vwapDistance < 0.3 && vwapClosePct > 85 {
		return "GO_LONG"
	}

	// 3. BEAR RUN CONDITIONS (Go Short)
	// Mirror of the bull loop: Price momentum drops below -3.0 due to signed direction.
	isBearExpansion := analytics.RollingPriceNormalized < -3
	isBelowValue := latestBar.Close < latestBar.VWAP

	if isBearish && isBearExpansion && isBelowValue && vwapDistance > -0.3 && vwapClosePct < 15 {
		return "GO_SHORT"
	}

	return "HOLD"
}

// CheckExit tracks structural momentum trend-breaks and distribution extensions
func (s *MomentumRunStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	history, ok := state.BarHistory["5m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// 1. Extract the current analytics metrics from the bar
	analytics := latestBar.Analytics
	rollingPrice := analytics.RollingPriceNormalized

	// 2. Evaluate exit conditions using your momentum distance boundary filters
	if currentSide == "LONG" {
		// Exit Long if it drops back below VWAP OR if rolling price momentum reverses completely (< -3.0)
		if rollingPrice < -3.0 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		// Exit Short if it bounces back above VWAP OR if rolling price momentum reverses completely (> 3.0)
		if rollingPrice > 3.0 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// CheckTakeProfit hooks up our automated configuration targets
func (s *MomentumRunStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return state.CurrentPnL >= s.cfg.TakeProfitINR
}

// CheckStopLoss hooks up our automated safety risk floor limits
func (s *MomentumRunStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return state.CurrentPnL <= s.cfg.HardStopLossINR
}

// OnEntryCommit modifies strategy-specific metadata states upon transaction entry confirmation
func (s *MomentumRunStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	state.ActiveStrategyName = s.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]StrategyStats)
	}

	stats := state.StrategyHistory[s.Name()]
	stats.IsCurrentlyActive = true
	state.StrategyHistory[s.Name()] = stats
}
