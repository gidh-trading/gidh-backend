package strategy

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
			TakeProfitINR:      1000.0, // Target Momentum Target Cap
			MaximumTradesCount: 2,      // Max Trades per stock limit rule
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
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	analytics := latestBar.Analytics
	const MinRankThreshold = 5

	// 1. Core Momentum Fuel Filters (Volume & Velocity)
	hasVolumeEffort := analytics.VolumeRank >= MinRankThreshold && analytics.RollingVolumeIntensity > 3
	hasTransactionVelocity := analytics.TickRank >= MinRankThreshold && analytics.RollingTickRank > 4.0

	// 2. Derive Current Raw & ADR-Normalized Distance from VWAP
	// (Using state profile metadata and the shared analytical helper formula)

	normalizedVwapDist := latestBar.Analytics.NormalizedVwapDistance
	vwapClosePct := latestBar.Analytics.VwapClosePct

	// 3. Determine Vector Directionality based on Price Expansion & Safety Bounds

	// BULL RUN CONDITIONS (Long Entry)
	isBullExpansion := analytics.PriceRank >= MinRankThreshold && analytics.RollingPriceNormalized > 3
	isAboveValue := latestBar.Close > latestBar.VWAP

	// New requested filters for high-momentum distribution extension:
	passesLongVwapPct := vwapClosePct > 80.0
	passesLongVwapDist := normalizedVwapDist > 0.1 && normalizedVwapDist < 0.2

	if hasVolumeEffort && hasTransactionVelocity && isBullExpansion && isAboveValue && passesLongVwapPct && passesLongVwapDist {
		return "GO_LONG"
	}

	// BEAR RUN CONDITIONS (Short Entry)
	isBearExpansion := analytics.PriceRank >= MinRankThreshold && analytics.RollingPriceNormalized < -4
	isBelowValue := latestBar.Close < latestBar.VWAP

	// New requested filters for downward momentum distribution breakdown:
	passesShortVwapPct := vwapClosePct < 20.0
	passesShortVwapDist := normalizedVwapDist < -0.1

	if hasVolumeEffort && hasTransactionVelocity && isBearExpansion && isBelowValue && passesShortVwapPct && passesShortVwapDist {
		return "GO_SHORT"
	}

	return "HOLD"
}

// CheckExit tracks structural momentum trend-breaks and distribution extensions
func (s *MomentumRunStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	// 1. Extract the current normalized VWAP distance from bar analytics
	normalizedVwapDist := latestBar.Analytics.NormalizedVwapDistance

	// 2. Evaluate exit conditions using your momentum distance boundary filter (0.1 / -0.1)
	if currentSide == "LONG" {
		// Exit Long if it drops back below VWAP OR stretches excessively past the positive momentum band (> 0.1)
		if normalizedVwapDist < -0.08 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		// Exit Short if it bounces back above VWAP OR stretches excessively past the negative momentum band (< -0.1)
		if normalizedVwapDist > 0.08 {
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
