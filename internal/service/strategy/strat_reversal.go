package strategy

type VwapEfficiencyReversalStrategy struct {
	strategyName string
}

func NewVwapEfficiencyReversalStrategy() *VwapEfficiencyReversalStrategy {
	return &VwapEfficiencyReversalStrategy{
		strategyName: "VWAP_Efficiency_Reversal",
	}
}

func (s *VwapEfficiencyReversalStrategy) Name() string {
	return s.strategyName
}

// CheckEntry looks up pre-calculated features inside the isolated timeframe history lookback buffer
func (s *VwapEfficiencyReversalStrategy) CheckEntry(state *InstrumentState) string {
	// Let's look at the standard execution timeframe (e.g., "1m")
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) < 2 {
		return "HOLD"
	}

	// Fetch the absolute latest completed bar containing frozen indicators
	latestBar := history[len(history)-1]

	// Extract the analytics properties computed cleanly by BarAnalyticsEngine
	slope := latestBar.Analytics.NetEfficiencySlope
	distance := latestBar.Analytics.NormalizedVwapDistance

	// Long Reversal Rule: Price breaks above VWAP with rising positive net efficiency slope
	if distance > 0.0 && slope > 0.05 {
		return "GO_LONG"
	}

	// Short Reversal Rule: Price breaks below VWAP with expanding negative net efficiency slope
	if distance < 0.0 && slope < -0.05 {
		return "GO_SHORT"
	}

	return "HOLD"
}

// CheckExit determines if structural parameters warrant shutting down active positions
func (s *VwapEfficiencyReversalStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	distance := latestBar.Analytics.NormalizedVwapDistance

	// Structural Reversal Exit Constraints
	if currentSide == "LONG" && distance < 0 {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && distance > 0 {
		return "EXIT_SHORT"
	}

	return "HOLD"
}

// CheckStopLoss reads the shallow portfolio performance variables to execute trailing stops
func (s *VwapEfficiencyReversalStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// Simple rule: If the open position drops past a specific percentage threshold relative to entry price
	if state.CurrentPnL < 0 {
		maxLossThresholdINR := -(avgPrice * 0.01) // 1% absolute hard stop limit
		if state.CurrentPnL <= maxLossThresholdINR {
			return true
		}
	}
	return false
}

// CheckTakeProfit handles automated mechanical trailing target locks
func (s *VwapEfficiencyReversalStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// Simple High-Confidence Trailing Safeguard:
	// If peak unrealized PnL moved up nicely, but has now given back 30% from the highs
	if state.PeakPnL > (avgPrice * 0.015) { // At least 1.5% profit was reached
		drawdownFromPeak := state.PeakPnL - state.CurrentPnL
		allowableGiveback := state.PeakPnL * 0.30
		if drawdownFromPeak >= allowableGiveback {
			return true // High confidence safety trail exit triggered
		}
	}
	return false
}
