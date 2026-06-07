package agent

import (
	"gidh-backend/pkg/logger"
	"time"
)

// GenerateSignal handles the primary routing logic using the Absorption State Machine.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	state, exists := sa.Registry[symbol]
	sa.mu.RUnlock()

	if !exists || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// 1. GLOBAL EMERGENCY RISK SHIELD (Hard Stop-Loss)
	if currentSide != "FLAT" && currentSide != "" {
		if sa.CheckGlobalEmergencyBrackets(state, entryPrice, currentSide) {
			state.CurrentSetupPhase = PhaseNeutral // Reset on hard stop
			if currentSide == "SHORT" {
				return "EXIT_SHORT"
			}
			return "EXIT_LONG"
		}
	}

	// 2. ACTIVE POSITION MANAGEMENT ("Enjoy the ride")
	if currentSide != "FLAT" && currentSide != "" {
		state.CurrentSetupPhase = PhaseActiveTrade
		return sa.evaluateAbsorptionExit(state, currentSide)
	}

	// Initialize state if empty
	if state.CurrentSetupPhase == "" {
		state.CurrentSetupPhase = PhaseNeutral
	}

	marketMins, tickTime := sa.getMarketMinutes(state.LastUpdated)

	// Session Close Check
	if marketMins > 900 { // Adjust to your exact session close time
		return sa.handleSessionCloseExits(currentSide)
	}

	// 3. ENTRY COOLDOWN
	if sa.isEngineInCooldown(state, tickTime) {
		state.CurrentSetupPhase = PhaseNeutral
		return "HOLD"
	}

	dir := string(state.LatestDirection)
	volRank := state.LatestVolumeRank

	// ========================================================================
	// THE PURE ORDER FLOW STATE MACHINE
	// ========================================================================
	switch state.CurrentSetupPhase {

	case PhaseNeutral:
		// TRIGGER: Look for the passive limit order footprint
		if dir == "BULLISH_ABSORPTION" {
			state.CurrentSetupPhase = PhaseBullishAbsorptionSpotted
			state.PhaseTimestamp = tickTime
			logger.Infof("[State Machine] BULLISH ABSORPTION on %s. Waiting for Buy Resolution.", state.Symbol)
		} else if dir == "BEARISH_ABSORPTION" {
			state.CurrentSetupPhase = PhaseBearishAbsorptionSpotted
			state.PhaseTimestamp = tickTime
			logger.Infof("[State Machine] BEARISH ABSORPTION on %s. Waiting for Sell Resolution.", state.Symbol)
		}
		return "HOLD"

	case PhaseBullishAbsorptionSpotted:
		// INVALIDATION: Timeout or flip
		if tickTime.Sub(state.PhaseTimestamp) > 5*time.Minute || dir == "STRONG_BEARISH" {
			state.CurrentSetupPhase = PhaseNeutral
			return "HOLD"
		}

		// EXTENSION: Refresh timer on new absorption
		if dir == "BULLISH_ABSORPTION" {
			state.PhaseTimestamp = tickTime
			return "HOLD"
		}

		// RESOLUTION: Market orders step in
		if (dir == "BULLISH" || dir == "STRONG_BULLISH") && volRank >= 6 {
			logger.Infof("[State Machine] LONG RESOLUTION Confirmed on %s! VolRank: %d", state.Symbol, volRank)
			return "GO_LONG"
		}
		return "HOLD"

	case PhaseBearishAbsorptionSpotted:
		// INVALIDATION: Timeout or flip
		if tickTime.Sub(state.PhaseTimestamp) > 5*time.Minute || dir == "STRONG_BULLISH" {
			state.CurrentSetupPhase = PhaseNeutral
			return "HOLD"
		}

		// EXTENSION: Refresh timer on new absorption
		if dir == "BEARISH_ABSORPTION" {
			state.PhaseTimestamp = tickTime
			return "HOLD"
		}

		// RESOLUTION: Market orders step in
		if (dir == "BEARISH" || dir == "STRONG_BEARISH") && volRank >= 6 {
			logger.Infof("[State Machine] SHORT RESOLUTION Confirmed on %s! VolRank: %d", state.Symbol, volRank)
			return "GO_SHORT"
		}
		return "HOLD"
	}

	return "HOLD"
}

// evaluateAbsorptionExit handles mid-trade management ("Enjoy the ride" until structural flip).
func (sa *ScalperAgent) evaluateAbsorptionExit(state *InstrumentState, currentSide string) string {
	recentTxs := sa.getRecentMinutesDataUnlocked(state, 1)

	if len(recentTxs) < 20 {
		return "HOLD"
	}

	bullCount := 0.0
	bearCount := 0.0

	for _, tx := range recentTxs {
		dir := string(tx.Direction)
		if dir == "BULLISH" || dir == "STRONG_BULLISH" {
			bullCount++
		} else if dir == "BEARISH" || dir == "STRONG_BEARISH" {
			bearCount++
		}
	}

	totalAggressive := bullCount + bearCount
	if totalAggressive == 0 {
		return "HOLD"
	}

	bullPct := bullCount / totalAggressive
	bearPct := bearCount / totalAggressive

	if currentSide == "LONG" && bearPct > 0.70 {
		state.CurrentSetupPhase = PhaseNeutral
		return "EXIT_LONG"
	}

	if currentSide == "SHORT" && bullPct > 0.70 {
		state.CurrentSetupPhase = PhaseNeutral
		return "EXIT_SHORT"
	}

	return "HOLD"
}
