package agent

import (
	"math"
	"time"
)

// GenerateSignal acts as the master conductor. It evaluates state and checks steps sequentially.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	state, exists := sa.Registry[symbol]
	sa.mu.RUnlock()

	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// Step 1: Is the market even open or past the cutoff?
	marketMins, tickTime := sa.getMarketMinutes(state.LastUpdated)
	if marketMins < 555 || marketMins > 630 {
		return sa.handleSessionCloseExits(currentSide)
	}

	// Step 2: Handle Active Position Management (Exits)
	if currentSide != "FLAT" && currentSide != "" {
		return sa.EvaluateActiveExits(state, entryPrice, currentSide)
	}

	// Step 3: Run Entry Guardrails (Cooldowns & Range building blocks)
	if sa.isEngineInCooldown(state, tickTime) {
		return "HOLD"
	}

	sa.UpdateOpeningRangeBoundaries(state, marketMins)
	if marketMins < 560 { // Block all trades between 9:15 and 9:20 AM
		return "HOLD"
	}

	// Step 4: Calculate Core Alpha Metrics
	vwapSlope := sa.calculateVwapSlope(state)
	bullWeight, bearWeight := sa.calculateVolumeWeights(state, 10, 6)

	// Step 5: Evaluate Strategy Rules Execution Step
	return sa.EvaluateMorningStrategyRules(state, vwapSlope, bullWeight, bearWeight)
}

// ------------------------------------------------------------------------
// STEP 1-3 UTILITIES: TIME, TIME-ZONES & BREAKOUT RANGE BOUNDARIES
// ------------------------------------------------------------------------

func (sa *ScalperAgent) getMarketMinutes(t time.Time) (int, time.Time) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err == nil {
		t = t.In(loc)
	}
	hour, minute, _ := t.Clock()
	return (hour * 60) + minute, t
}

func (sa *ScalperAgent) handleSessionCloseExits(currentSide string) string {
	if currentSide == "SHORT" {
		return "EXIT_SHORT"
	}
	if currentSide == "LONG" {
		return "EXIT_LONG"
	}
	return "HOLD"
}

func (sa *ScalperAgent) isEngineInCooldown(state *InstrumentState, currentTickTime time.Time) bool {
	return !state.LastExitTime.IsZero() && currentTickTime.Sub(state.LastExitTime) < 5*time.Minute
}

func (sa *ScalperAgent) UpdateOpeningRangeBoundaries(state *InstrumentState, marketMins int) {
	if marketMins >= 555 && marketMins < 560 {
		if !state.OpeningRangeSet {
			state.OpeningHigh = state.LatestPrice
			state.OpeningLow = state.LatestPrice
			state.OpeningRangeSet = true
		} else {
			if state.LatestPrice > state.OpeningHigh {
				state.OpeningHigh = state.LatestPrice
			}
			if state.LatestPrice < state.OpeningLow {
				state.OpeningLow = state.LatestPrice
			}
		}
	}
}

// ------------------------------------------------------------------------
// STEP 4 UTILITIES: MATH INDICATORS & TAPE VOLUMES
// ------------------------------------------------------------------------

func (sa *ScalperAgent) calculateVwapSlope(state *InstrumentState) float64 {
	if state.PrevSessionVWAP == 0 {
		state.PrevSessionVWAP = state.LatestSessionVWAP
		return 0.0
	}
	slope := state.LatestSessionVWAP - state.PrevSessionVWAP
	state.PrevSessionVWAP = state.LatestSessionVWAP // update state memory frame
	return slope
}

func (sa *ScalperAgent) calculateVolumeWeights(state *InstrumentState, ticksLookback int, minRank int) (float64, float64) {
	recentTicks := sa.getLastTransactionsUnlocked(state, ticksLookback)
	var bullWeight, bearWeight float64

	for _, tx := range recentTicks {
		if tx.VolumeRank >= minRank {
			switch tx.Direction {
			case "BULLISH", "STRONG_BULLISH", "BULLISH_ABSORPTION":
				bullWeight += tx.Volume
			case "BEARISH", "STRONG_BEARISH", "BEARISH_ABSORPTION":
				bearWeight += tx.Volume
			}
		}
	}
	return bullWeight, bearWeight
}

// EvaluateMorningStrategyRules serves as your clean canvas to write trading rules.
// Each rule is broken down as an explicit boolean step.
func (sa *ScalperAgent) EvaluateMorningStrategyRules(
	state *InstrumentState,
	vwapSlope float64,
	bullWeight float64,
	bearWeight float64,
) string {

	// STEP A: Structural Anchor Verification
	if !state.OpeningRangeSet || state.OpeningHigh == 0 {
		return "HOLD"
	}

	// STEP B: Chop Zone / Centroid Proximity Check
	distFromVWAP := ((state.LatestPrice - state.LatestSessionVWAP) / state.LatestSessionVWAP) * 100
	if math.Abs(distFromVWAP) < 0.15 {
		return "HOLD" // Price is hovering inside the high-churn fair value zone
	}

	// ------------------------------------------------------------------------
	// SHORT STRATEGY PATTERN (Institutional Breakdown Setup)
	// ------------------------------------------------------------------------
	isBelowVWAP := state.LatestPrice < state.LatestSessionVWAP
	isVwapSlopingDown := vwapSlope < 0
	isOpeningLowBroken := state.LatestPrice < state.OpeningLow
	isBearishTapeDominant := bearWeight > (bullWeight * 1.5)

	// Combine components into a transparent execution gate
	if isBelowVWAP && isVwapSlopingDown && isOpeningLowBroken && isBearishTapeDominant {
		return "GO_SHORT"
	}

	// ------------------------------------------------------------------------
	// LONG STRATEGY PATTERN (Institutional Breakout Setup)
	// ------------------------------------------------------------------------
	isAboveVWAP := state.LatestPrice > state.LatestSessionVWAP
	isVwapSlopingUp := vwapSlope > 0
	isOpeningHighBroken := state.LatestPrice > state.OpeningHigh
	isBullishTapeDominant := bullWeight > (bearWeight * 1.5)

	// Combine components into a transparent execution gate
	if isAboveVWAP && isVwapSlopingUp && isOpeningHighBroken && isBullishTapeDominant {
		return "GO_LONG"
	}

	return "HOLD"
}

func (sa *ScalperAgent) EvaluateActiveExits(state *InstrumentState, entryPrice float64, currentSide string) string {
	if currentSide == "SHORT" {
		if state.LatestDirection == "BULLISH_ABSORPTION" || state.LatestDirection == "STRONG_BULLISH" {
			return "EXIT_SHORT"
		}
		if sa.EvaluateDualQueueBrackets(state, entryPrice, "SHORT") {
			return "EXIT_SHORT"
		}
	}

	if currentSide == "LONG" {
		if state.LatestDirection == "BEARISH_ABSORPTION" || state.LatestDirection == "STRONG_BEARISH" {
			return "EXIT_LONG"
		}
		if sa.EvaluateDualQueueBrackets(state, entryPrice, "LONG") {
			return "EXIT_LONG"
		}
	}

	return "HOLD"
}
