package agent

import (
	"gidh-backend/pkg/logger"
)

// generateMorningSignal implements Phase 1 (9:15 AM - 10:30 AM) of the Feudal Age strategy.
// generateMorningSignal implements Phase 1 (9:15 AM - 10:30 AM) of the Feudal Age strategy.
func (sa *ScalperAgent) generateMorningSignal(state *InstrumentState, currentSide string) string {
	// 1. TIME BOUNDARY GUARDRAILS
	marketMins, tickTime := sa.getMarketMinutes(state.LastUpdated)
	if marketMins < 555 || marketMins > 630 {
		return sa.handleSessionCloseExits(currentSide)
	}

	// 2. ACTIVE POSITION MANAGEMENT
	if currentSide != "FLAT" && currentSide != "" {
		return sa.evaluateActivePositionExit(state, currentSide)
	}

	// 3. ENTRY PROTECTION CONTROLS
	if sa.isEngineInCooldown(state, tickTime) {
		return "HOLD"
	}

	sa.UpdateOpeningRangeBoundaries(state, marketMins)

	// Changed from 570 (9:30 AM) to 560 (9:20 AM) so it doesn't wait forever
	if marketMins < 560 || !state.OpeningRangeSet || state.OpeningHigh == 0 {
		return "HOLD"
	}

	// 4. THE STRICT MASTER TREND FILTER (Gatekeeper)
	isAboveVWAP := state.LatestPrice > state.LatestSessionVWAP
	isBelowVWAP := state.LatestPrice < state.LatestSessionVWAP

	// 5. CALCULATE CONVICTION SCORING
	obiRatio := sa.calculateOBIRatio(state.LatestTotalBuyQuantity, state.LatestTotalSellQuantity)

	longScore := sa.calculateLongConviction(state, obiRatio)
	shortScore := sa.calculateShortConviction(state, obiRatio)

	// 6. STRATEGY ENTRY EVALUATION (Threshold lowered to 6 while OBI is mocked)
	// We removed the strict 'LatestDirection' check because the Breakout + VWAP filter is enough proof.
	if isAboveVWAP && longScore >= 6 {
		logger.Infof("[Feudal Morning] LONG Triggered for %s. Score: %d/10 | OBI: %.2f | Change: %.2f%%",
			state.Symbol, longScore, obiRatio, state.LatestChangePct)
		return "GO_LONG"
	}

	if isBelowVWAP && shortScore >= 6 {
		logger.Infof("[Feudal Morning] SHORT Triggered for %s. Score: %d/10 | OBI: %.2f | Change: %.2f%%",
			state.Symbol, shortScore, obiRatio, state.LatestChangePct)
		return "GO_SHORT"
	}

	return "HOLD"
}

// ========================================================================
// 📊 ISOLATED CONVICTION SCORING ENGINES
// ========================================================================

// calculateLongConviction sums up the points based on structural rules for Long trade setups.
func (sa *ScalperAgent) calculateLongConviction(state *InstrumentState, obiRatio float64) int {
	score := 0

	// Rule 1: Resolution Breakout (+3 Points)
	// We check if the LAST CLOSED 1-minute bar actually closed above the Opening High.
	if bars, exists := state.BarHistory["1m"]; exists && len(bars) > 0 {
		lastClosedBar := bars[len(bars)-1]
		if lastClosedBar.Close > state.OpeningHigh {
			score += 3
		}
	} else {
		// Fallback if bars haven't populated yet
		if state.LatestPrice > state.OpeningHigh {
			score += 3
		}
	}

	// Rule 2: Order Book Imbalance (+2 Points)
	if obiRatio >= 0.1 {
		score += 2
	}

	// Rule 3: Price Rank Filter (+2 Points)
	if state.LatestPriceRank >= 5 {
		score += 2
	}

	// Rule 4: Volume Surge (+3 Points)
	if state.LatestVolumeRank >= 6 {
		score += 3
	}

	return score
}

// calculateShortConviction sums up the points based on structural rules for Short trade setups.
func (sa *ScalperAgent) calculateShortConviction(state *InstrumentState, obiRatio float64) int {
	score := 0

	// Rule 1: Resolution Breakout (+3 Points)
	if bars, exists := state.BarHistory["1m"]; exists && len(bars) > 0 {
		lastClosedBar := bars[len(bars)-1]
		if lastClosedBar.Close < state.OpeningLow {
			score += 3
		}
	} else {
		if state.LatestPrice < state.OpeningLow {
			score += 3
		}
	}

	// Rule 2: Order Book Imbalance (+2 Points)
	if obiRatio <= -0.1 {
		score += 2
	}

	// Rule 3: Price Rank Filter (+2 Points)
	if state.LatestPriceRank >= 5 {
		score += 2
	}

	// Rule 4: Volume Surge (+3 Points)
	if state.LatestVolumeRank >= 6 {
		score += 3
	}

	return score
}

// ========================================================================
// ⚙️ STRATEGY UTILITY HELPERS
// ========================================================================

func (sa *ScalperAgent) calculateOBIRatio(tBq, tSq int64) float64 {
	fBq := float64(tBq)
	fSq := float64(tSq)
	if (fBq + fSq) == 0 {
		return 0.0
	}
	return (fBq - fSq) / (fBq + fSq)
}

// evaluateActivePositionExit handles mid-trade management using Time-Based Memory.
func (sa *ScalperAgent) evaluateActivePositionExit(state *InstrumentState, currentSide string) string {
	// Pull the last 1 full minute of ticks, regardless of how many transactions there were.
	recentTxs := sa.getRecentMinutesDataUnlocked(state, 1)

	// If we don't have enough time built up (e.g., less than 30 ticks in a minute), HOLD.
	if len(recentTxs) < 30 {
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

	total := float64(len(recentTxs))
	bullPct := bullCount / total
	bearPct := bearCount / total

	// Require a sustained 1-minute structural shift to warrant an early exit
	if currentSide == "LONG" {
		if bearPct > 0.65 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if bullPct > 0.65 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}
