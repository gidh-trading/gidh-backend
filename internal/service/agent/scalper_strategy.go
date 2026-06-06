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

	// ------------------------------------------------------------------------
	// STEP 1: TIME BOUNDARY GUARDRAILS
	// ------------------------------------------------------------------------
	marketMins, tickTime := sa.getMarketMinutes(state.LastUpdated)
	if marketMins < 555 || marketMins > 630 {
		return sa.handleSessionCloseExits(currentSide)
	}

	// ------------------------------------------------------------------------
	// STEP 2: ACTIVE POSITION MANAGEMENT (Inlined Tape Resolution & Risk Brackets)
	// ------------------------------------------------------------------------
	if currentSide != "FLAT" && currentSide != "" {
		dir := string(state.LatestDirection)

		if currentSide == "SHORT" {
			// A. If opposite absorption prints on the ribbon, we WAIT for resolution
			if dir == "BULLISH_ABSORPTION" {
				return "HOLD"
			}

			// B. RESOLUTION TRIGGER: Passive buyers turn aggressive and lift the offers
			if dir == "BULLISH" || dir == "STRONG_BULLISH" {
				return "EXIT_SHORT"
			}

			// C. RISK BACKSTOP: Emergency structural/volatility bracket breach
			if sa.EvaluateDualQueueBrackets(state, entryPrice, "SHORT") {
				return "EXIT_SHORT"
			}
		}

		if currentSide == "LONG" {
			// A. If overhead passive sellers step in, hold and wait for resolution
			if dir == "BEARISH_ABSORPTION" {
				return "HOLD"
			}

			// B. RESOLUTION TRIGGER: Sellers overwhelm the order block and hit bids aggressively
			if dir == "BEARISH" || dir == "STRONG_BEARISH" {
				return "EXIT_LONG"
			}

			// C. RISK BACKSTOP: Emergency structural/volatility bracket breach
			if sa.EvaluateDualQueueBrackets(state, entryPrice, "LONG") {
				return "EXIT_LONG"
			}
		}

		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 3: ENTRY GUARDRAILS (Post-Trade Cooldowns & Range Discovery)
	// ------------------------------------------------------------------------
	if sa.isEngineInCooldown(state, tickTime) {
		return "HOLD"
	}

	sa.UpdateOpeningRangeBoundaries(state, marketMins)
	if marketMins < 560 { // Strict observation lockout from 9:15 AM to 9:20 AM
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 4: ENTRY STRATEGY RULES MATRIX (Direction & Volume-Driven)
	// ------------------------------------------------------------------------
	if !state.OpeningRangeSet || state.OpeningHigh == 0 {
		return "HOLD"
	}

	// Chop Zone Filter: Avoid trading too close to the session average baseline
	distFromVWAP := ((state.LatestPrice - state.LatestSessionVWAP) / state.LatestSessionVWAP) * 100
	if math.Abs(distFromVWAP) < 0.15 {
		return "HOLD"
	}

	// Establish institutional baseline parameters
	isHighVolumeParticipant := state.LatestVolumeRank >= 6
	dir := string(state.LatestDirection)

	// --- SHORT PATTERN CRITERIA ---
	isBelowVWAP := state.LatestPrice < state.LatestSessionVWAP
	isOpeningLowBroken := state.LatestPrice < state.OpeningLow
	isBearishTape := dir == "BEARISH" || dir == "STRONG_BEARISH"

	if isBelowVWAP && isOpeningLowBroken && isHighVolumeParticipant && isBearishTape {
		return "GO_SHORT"
	}

	// --- LONG PATTERN CRITERIA ---
	isAboveVWAP := state.LatestPrice > state.LatestSessionVWAP
	isOpeningHighBroken := state.LatestPrice > state.OpeningHigh
	isBullishTape := dir == "BULLISH" || dir == "STRONG_BULLISH"

	if isAboveVWAP && isOpeningHighBroken && isHighVolumeParticipant && isBullishTape {
		return "GO_LONG"
	}

	return "HOLD"
}

// ------------------------------------------------------------------------
// STEP 4: ENTRY STRATEGY RULES MATRIX (Direction-Driven Open)
// ------------------------------------------------------------------------

func (sa *ScalperAgent) EvaluateMorningStrategyRules(state *InstrumentState, minRank int) string {
	// Structural Anchor Verification
	if !state.OpeningRangeSet || state.OpeningHigh == 0 {
		return "HOLD"
	}

	// Chop Zone / Centroid Proximity Check
	distFromVWAP := ((state.LatestPrice - state.LatestSessionVWAP) / state.LatestSessionVWAP) * 100
	if math.Abs(distFromVWAP) < 0.15 {
		return "HOLD" // Price is hovering inside the high-churn fair value zone
	}

	// Microscopic Order Flow Checks
	isHighVolumeParticipant := state.LatestVolumeRank >= minRank
	dir := string(state.LatestDirection)

	// --- SHORT PATTERN CRITERIA ---
	isBelowVWAP := state.LatestPrice < state.LatestSessionVWAP
	isOpeningLowBroken := state.LatestPrice < state.OpeningLow
	isBearishTape := dir == "BEARISH" || dir == "STRONG_BEARISH"

	if isBelowVWAP && isOpeningLowBroken && isHighVolumeParticipant && isBearishTape {
		return "GO_SHORT"
	}

	// --- LONG PATTERN CRITERIA ---
	isAboveVWAP := state.LatestPrice > state.LatestSessionVWAP
	isOpeningHighBroken := state.LatestPrice > state.OpeningHigh
	isBullishTape := dir == "BULLISH" || dir == "STRONG_BULLISH"

	if isAboveVWAP && isOpeningHighBroken && isHighVolumeParticipant && isBullishTape {
		return "GO_LONG"
	}

	return "HOLD"
}

// ------------------------------------------------------------------------
// STEP 5: ACTIVE POSITION MANAGEMENT (Tape Resolution Exits)
// ------------------------------------------------------------------------

func (sa *ScalperAgent) EvaluateActiveExits(state *InstrumentState, entryPrice float64, currentSide string) string {
	dir := string(state.LatestDirection)

	// --- POSITION IS SHORT ---
	if currentSide == "SHORT" {
		// 1. If counter-absorption is printing on the ribbon, we WAIT for resolution
		if dir == "BULLISH_ABSORPTION" {
			return "HOLD"
		}

		// 2. RESOLUTION ACCELERATION: Buyers break through the block and absorb completely
		if dir == "BULLISH" || dir == "STRONG_BULLISH" {
			return "EXIT_SHORT"
		}

		// 3. Mathematical Risk Guardrails Fallback
		if sa.EvaluateDualQueueBrackets(state, entryPrice, "SHORT") {
			return "EXIT_SHORT"
		}
	}

	// --- POSITION IS LONG ---
	if currentSide == "LONG" {
		// 1. If overhead passive sellers step in, hold and look for absorption breakdown
		if dir == "BEARISH_ABSORPTION" {
			return "HOLD"
		}

		// 2. RESOLUTION ACCELERATION: Sellers overpower order blocks completely
		if dir == "BEARISH" || dir == "STRONG_BEARISH" {
			return "EXIT_LONG"
		}

		// 3. Mathematical Risk Guardrails Fallback
		if sa.EvaluateDualQueueBrackets(state, entryPrice, "LONG") {
			return "EXIT_LONG"
		}
	}

	return "HOLD"
}

// RegisterPositionClosure logs trade updates to fire the Cooldown module engine
func (sa *ScalperAgent) RegisterPositionClosure(symbol string, completionTime time.Time) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if state, exists := sa.Registry[symbol]; exists {
		state.LastExitTime = completionTime
	}
}
