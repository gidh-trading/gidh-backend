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

	// Rule 1: Opening Range Breakout (+3 Points)
	if state.LatestPrice > state.OpeningHigh {
		score += 3
	}

	// Rule 2: Order Book Imbalance (+3 Points)
	if obiRatio >= 0.1 {
		score += 2
	}

	// Rule 3: Price Rank Filter (+2 Points)
	if state.LatestPriceRank >= 5 {
		score += 2
	}

	// Rule 4: Volume Surge (+2 Points)
	if state.LatestVolumeRank >= 6 {
		score += 3
	}

	return score
}

// calculateShortConviction sums up the points based on structural rules for Short trade setups.
func (sa *ScalperAgent) calculateShortConviction(state *InstrumentState, obiRatio float64) int {
	score := 0

	// Rule 1: Opening Range Breakout (+3 Points)
	if state.LatestPrice < state.OpeningLow {
		score += 3
	}

	// Rule 2: Order Book Imbalance (+3 Points)
	if obiRatio <= -0.1 {
		score += 2
	}

	// Rule 3: Price Rank Filter (+2 Points)
	if state.LatestPriceRank >= 5 {
		score += 2
	}

	// Rule 4: Volume Surge (+2 Points)
	if state.LatestVolumeRank >= 6 {
		score += 3
	}

	return score
}

// ========================================================================
// ⚙️ STRATEGY UTILITY HELPERS
// ========================================================================

// calculateOBIRatio safely computes the standard Order Book Imbalance ratio.
func (sa *ScalperAgent) calculateOBIRatio(tBq, tSq int64) float64 {
	return float64((tBq - tSq) / (tBq + tSq))
}

// evaluateActivePositionExit handles mid-trade management via inline tape indicators.
func (sa *ScalperAgent) evaluateActivePositionExit(state *InstrumentState, currentSide string) string {
	dir := string(state.LatestDirection)

	if currentSide == "SHORT" {
		if dir == "BULLISH_ABSORPTION" {
			return "HOLD"
		}
		if dir == "BULLISH" || dir == "STRONG_BULLISH" {
			return "EXIT_SHORT"
		}
	}

	if currentSide == "LONG" {
		if dir == "BEARISH_ABSORPTION" {
			return "HOLD"
		}
		if dir == "BEARISH" || dir == "STRONG_BEARISH" {
			return "EXIT_LONG"
		}
	}

	return "HOLD"
}
