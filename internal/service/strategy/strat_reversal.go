package strategy

type VwapEfficiencyMomentumStrategy struct {
	strategyName string

	// Momentum Config Thresholds
	effIgnitionThreshold  float64 // Minimum net efficiency to validate active momentum (e.g. 15.0 to 20.0)
	slopeTriggerThreshold float64 // Rate of change threshold for linear acceleration (e.g. 1.0 to 1.5)
	maxVwapExtension      float64 // Prevent chasing overextended bars (e.g. 0.5 to 0.7 ADR units)
	exhaustionThreshold   float64 // Ceiling where trend structural energy caps out (e.g. 80.0)
}

func NewVwapEfficiencyMomentumStrategy() *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName:          "VWAP_Efficiency_Momentum",
		effIgnitionThreshold:  15.0,
		slopeTriggerThreshold: 1.0,
		maxVwapExtension:      0.6,
		exhaustionThreshold:   80.0,
	}
}

func (s *VwapEfficiencyMomentumStrategy) Name() string {
	return s.strategyName
}

// CheckEntry looks up momentum explosion patterns inside the isolated timeframe lookback
func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) < 2 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]

	// Extract features cleanly computed by your BarAnalyticsEngine
	netEff := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope
	distance := latestBar.Analytics.NormalizedVwapDistance
	volumeRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank

	// FILTER: Require authentic institutional volume footprint (Rank >= 4) to ensure liquidity
	if volumeRank < 6 || priceRank < 5 {
		return "HOLD"
	}

	// 🚀 LONG MOMENTUM IGNITION
	// Efficiency breaks positive + high directional acceleration + price is near but above VWAP
	if netEff > s.effIgnitionThreshold && slope >= s.slopeTriggerThreshold {
		if distance > 0.0 && distance <= s.maxVwapExtension {
			return "GO_LONG"
		}
	}

	// 📉 SHORT MOMENTUM IGNITION
	// Efficiency breaks negative + high negative acceleration + price is near but below VWAP
	if netEff < -s.effIgnitionThreshold && slope <= -s.slopeTriggerThreshold {
		if distance < 0.0 && distance >= -s.maxVwapExtension {
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

// CheckExit shifts from a simple VWAP cross to cutting trends when speed completely dies
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	netEff := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope
	distance := latestBar.Analytics.NormalizedVwapDistance

	// 1. HARD CROSS CUT: If the asset crashes completely through the VWAP anchor floor
	if currentSide == "LONG" && distance < -0.05 {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && distance > 0.05 {
		return "EXIT_SHORT"
	}

	// 2. MOMENTUM EXHAUSTION EXIT:
	// If the trend is overbought/oversold, and the acceleration slope rounds off or reverses sign
	if currentSide == "LONG" {
		if netEff > s.exhaustionThreshold && slope < 0.2 {
			return "EXIT_LONG" // Exit at the peak before distribution happens
		}
		if netEff < 0.0 {
			return "EXIT_LONG" // Exit if energy balance flips negative
		}
	}

	if currentSide == "SHORT" {
		if netEff < -s.exhaustionThreshold && slope > -0.2 {
			return "EXIT_SHORT" // Exit at bottom climax before absorption rally
		}
		if netEff > 0.0 {
			return "EXIT_SHORT" // Exit if energy balance flips positive
		}
	}

	return "HOLD"
}

// CheckStopLoss reads shallow risk parameters to protect trade principal
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.CurrentPnL < 0 {
		// Tight momentum risk profile: 0.75% structural limit for rapid execution adjustments
		maxLossThresholdINR := -(avgPrice * 0.0075)
		if state.CurrentPnL <= maxLossThresholdINR {
			return true
		}
	}
	return false
}

// CheckTakeProfit handles automated technical trailing profit protections
func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// Trailing Momentum Protection Rule:
	// If the move exploded nicely (e.g., reached > 1.2%), protect the gains if they pull back 25% from the high water mark
	if state.PeakPnL > (avgPrice * 0.012) {
		drawdownFromPeak := state.PeakPnL - state.CurrentPnL
		allowableGiveback := state.PeakPnL * 0.25
		if drawdownFromPeak >= allowableGiveback {
			return true
		}
	}
	return false
}
