package strategy

import "gidh-backend/internal/service/models"

func CheckStatisticalTakeProfit(
	state *InstrumentState,
	pct *models.VWAPDistancePercentile,
	currentSide string,
	averagePrice float64,
	netQty int,
	baseProfit float64,
) bool {
	// 1. If data is missing, fail-safe to a standard profit targets
	if pct == nil || netQty == 0 || averagePrice <= 0 {
		return state.CurrentPnL >= baseProfit
	}

	// 2. Track the peak trade PnL achieved so far
	if state.CurrentPnL > state.MaxPnL {
		state.MaxPnL = state.CurrentPnL
	}

	// 3. Determine the statistical price expansion ceilings for this specific stock
	var targetP75PriceDiff, targetP90PriceDiff, targetP97PriceDiff float64

	if currentSide == "LONG" {
		// Positive extensions pool represents direct percentages or price offsets above VWAP
		targetP75PriceDiff = pct.PosP75
		targetP90PriceDiff = pct.PosP90
		targetP97PriceDiff = pct.PosP97
	} else if currentSide == "SHORT" {
		// Negative extensions pool (stored as absolute magnitudes)
		targetP75PriceDiff = pct.NegP75
		targetP90PriceDiff = pct.NegP90
		targetP97PriceDiff = pct.NegP97
	}

	// 4. Convert price difference thresholds into exact PnL cash milestones
	sharesCount := float64(netQty)
	if sharesCount < 0 {
		sharesCount = -sharesCount
	}

	baseActivationTarget := targetP75PriceDiff * sharesCount // Activation Milestone (P75)
	strongRunMilestone := targetP90PriceDiff * sharesCount   // Strong Run Milestone (P90)
	exhaustionMilestone := targetP97PriceDiff * sharesCount  // Blown-out Top Milestone (P97)

	// 5. If we haven't even crossed the baseline historical 75th percentile extension, ride the trend
	if state.MaxPnL < baseActivationTarget {
		return false
	}

	var dynamicFloor float64

	// 6. Adaptive Bracket Evaluation
	switch {
	case state.MaxPnL >= exhaustionMilestone:
		// Extreme Run: Price is in the top 3% of historical moves.
		// Lock it down aggressively (Protect 90% of peak) because a fast mean-reversion collapse is highly likely.
		dynamicFloor = state.MaxPnL * 0.90

	case state.MaxPnL >= strongRunMilestone:
		// Strong Run: Price cleared the 90th percentile. Protect 80% of peak.
		dynamicFloor = state.MaxPnL * 0.80

	default:
		// Base Target (P75 to P90 range): Give it breathing room to handle micro-pullbacks.
		// Guarantee we lock in at least 70% of the P75 baseline valuation.
		dynamicFloor = baseActivationTarget * 0.70
	}

	// 7. Exit if momentum stalls and drops through our mathematical distribution floor
	return state.CurrentPnL <= dynamicFloor
}

func CheckDynamicATRStopLoss(
	state *InstrumentState,
	profile *models.InstrumentProfile,
	netQty int,
) bool {
	// 1. If no active position or missing profile data, look at the hardcoded safety net
	if netQty == 0 || profile == nil || profile.ATR14 <= 0 {
		return state.CurrentPnL <= -700.0 // fallback floor
	}

	// 2. Calculate the maximum acceptable adverse price move (e.g., 1.5 * ATR)
	// For a stock with ATR of 10, we allow it to go 15 points against us before cutting.
	maxPriceAdverseMove := profile.ATR14 * 1.5

	// 3. Convert that price risk to a cash (INR) risk limit based on our current share count
	// Absolute value ensures netQty works whether we are LONG (positive) or SHORT (negative)
	sharesCount := float64(netQty)
	if sharesCount < 0 {
		sharesCount = -sharesCount
	}

	dynamicStopLossINR := -(maxPriceAdverseMove * sharesCount)

	// 4. Trigger exit if our current trade PnL breaches this customized risk floor
	return state.CurrentPnL <= dynamicStopLossINR
}
