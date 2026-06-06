package agent

import (
	"math"
)

// GenerateSignal handles the Morning Momentum Flow (MMF) strategy using order flow and global brackets.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	state, exists := sa.Registry[symbol]
	sa.mu.RUnlock()

	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 1: GLOBAL CAPITAL EMERGENCY RISK SHIELD
	// ------------------------------------------------------------------------
	if currentSide != "FLAT" && currentSide != "" {
		if sa.CheckGlobalEmergencyBrackets(state, entryPrice, currentSide) {
			if currentSide == "SHORT" {
				return "EXIT_SHORT"
			}
			if currentSide == "LONG" {
				return "EXIT_LONG"
			}
		}
	}

	// ------------------------------------------------------------------------
	// STEP 2: TIME BOUNDARY GUARDRAILS
	// ------------------------------------------------------------------------
	marketMins, tickTime := sa.getMarketMinutes(state.LastUpdated)
	if marketMins < 555 || marketMins > 630 {
		return sa.handleSessionCloseExits(currentSide)
	}

	// ------------------------------------------------------------------------
	// STEP 3: STRATEGY ACTIVE POSITION MANAGEMENT (Inline Tape Resolution Exits)
	// ------------------------------------------------------------------------
	if currentSide != "FLAT" && currentSide != "" {
		dir := string(state.LatestDirection)

		if currentSide == "SHORT" {
			// A. Passive limit floor discovered: WAIT for resolution
			if dir == "BULLISH_ABSORPTION" {
				return "HOLD"
			}
			// B. Resolution verified: Passive buyers turn aggressive and sweep offers
			if dir == "BULLISH" || dir == "STRONG_BULLISH" {
				return "EXIT_SHORT"
			}
		}

		if currentSide == "LONG" {
			// A. Overhead passive selling barrier encountered: WAIT for resolution
			if dir == "BEARISH_ABSORPTION" {
				return "HOLD"
			}
			// B. Resolution verified: Passive sellers hit bids with aggressive market orders
			if dir == "BEARISH" || dir == "STRONG_BEARISH" {
				return "EXIT_LONG"
			}
		}

		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 4: STRATEGY ENTRY PROTECTION CONTROLS (Cooldowns & Range Discovery)
	// ------------------------------------------------------------------------
	if sa.isEngineInCooldown(state, tickTime) {
		return "HOLD"
	}

	sa.UpdateOpeningRangeBoundaries(state, marketMins)
	if marketMins < 560 { // Observational lockout period (9:15 AM - 9:20 AM)
		return "HOLD"
	}

	if !state.OpeningRangeSet || state.OpeningHigh == 0 {
		return "HOLD"
	}

	// Central fair value chop zone filtering
	distFromVWAP := ((state.LatestPrice - state.LatestSessionVWAP) / state.LatestSessionVWAP) * 100
	if math.Abs(distFromVWAP) < 0.15 {
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 5: CORE TAPE STRATEGY ENTRYS
	// ------------------------------------------------------------------------
	isHighVolumeParticipant := state.LatestVolumeRank >= 6
	dir := string(state.LatestDirection)

	// --- SHORT SIGNAL GATING ---
	isBelowVWAP := state.LatestPrice < state.LatestSessionVWAP
	isOpeningLowBroken := state.LatestPrice < state.OpeningLow
	isBearishTape := dir == "BEARISH" || dir == "STRONG_BEARISH"

	if isBelowVWAP && isOpeningLowBroken && isHighVolumeParticipant && isBearishTape {
		return "GO_SHORT"
	}

	// --- LONG SIGNAL GATING ---
	isAboveVWAP := state.LatestPrice > state.LatestSessionVWAP
	isOpeningHighBroken := state.LatestPrice > state.OpeningHigh
	isBullishTape := dir == "BULLISH" || dir == "STRONG_BULLISH"

	if isAboveVWAP && isOpeningHighBroken && isHighVolumeParticipant && isBullishTape {
		return "GO_LONG"
	}

	return "HOLD"
}
