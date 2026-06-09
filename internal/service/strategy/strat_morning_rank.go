package strategy

import (
	"gidh-backend/internal/service/models"
)

type MorningRankStrategy struct {
	FixedProfitTargetINR float64
	MaxLossTargetINR     float64
}

func NewMorningRankStrategy() *MorningRankStrategy {
	return &MorningRankStrategy{
		FixedProfitTargetINR: 2000.0, // Hardcoded cash target goal
		MaxLossTargetINR:     600.0,  // Risk barrier cap
	}
}

func (s *MorningRankStrategy) Name() string { return "Morning_4Method_Ranks" }

func (s *MorningRankStrategy) CheckEntry(state *InstrumentState) string {
	bars1m := state.BarHistory["1m"]
	// We need at least 2 bars so we can safely extract the stable, closed VWAP of the previous minute
	if len(bars1m) < 2 {
		return "HOLD"
	}

	// ⏱️ ANTI-OVERTRADING FILTER 1: Strict Morning Time Window
	// Restrict entries strictly to the high-velocity opening drive (9:15 AM - 9:45 AM).
	// This prevents chasing low-liquidity chopped ranges later in the day.
	if state.MinutesSinceOpen < 0 || state.MinutesSinceOpen > 30 {
		return "HOLD"
	}

	latestClosedBar := bars1m[len(bars1m)-1]
	previousClosedBar := bars1m[len(bars1m)-2] // Our stable anchor line

	// Extract previous closed bar's structural VWAP to eliminate morning tick fluctuation
	stableAnchorVwap := previousClosedBar.VWAP
	if stableAnchorVwap <= 0 {
		return "HOLD"
	}

	analytics := latestClosedBar.Analytics

	// 📊 ANTI-OVERTRADING FILTER 2: Extreme Capital Commitment & Price Agreement
	// We only trigger if volume is top-tier (Rank 7 -> P97+)
	// AND the price body shows clear, heavy institutional expansion (Rank >= 6 -> P90+).
	// This ensures we never trade minor wiggles or high-volume stalled bars.
	isInstitutionalShock := analytics.VolumeRank >= 7
	isPriceExpanding := analytics.PriceRank >= 6

	if isInstitutionalShock && isPriceExpanding {

		// --- 🟢 EVALUATE LONG DIRECTION ---
		// Price closed cleanly ABOVE our stable historical VWAP anchor line
		if latestClosedBar.Close > stableAnchorVwap {
			if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
				return "GO_LONG"
			}
		}

		// --- 🔴 EVALUATE SHORT DIRECTION ---
		// Price closed cleanly BELOW our stable historical VWAP anchor line
		if latestClosedBar.Close < stableAnchorVwap {
			if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
				return "GO_SHORT"
			}
		}
	}

	return "HOLD"
}

func (s *MorningRankStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	bars1m := state.BarHistory["1m"]
	if len(bars1m) == 0 {
		return "HOLD"
	}

	latestClosedBar := bars1m[len(bars1m)-1]
	analytics := latestClosedBar.Analytics

	// --- 🟢 EVALUATE LONG POSITION EXITS ---
	if currentSide == "LONG" {
		// Exit ONLY if the opposing downward move is backed by heavy size (Rank >= 6 -> P90+)
		// AND the price cleanly expands downwards against us.
		// We completely ignore Bearish Absorption walls here to let the cluster resolve naturally.
		if analytics.VolumeRank >= 6 {
			if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
				return "EXIT_LONG"
			}
		}
	}

	// --- 🔴 EVALUATE SHORT POSITION EXITS ---
	if currentSide == "SHORT" {
		// Exit ONLY if the opposing upward move is backed by heavy size (Rank >= 6 -> P90+)
		// AND the price cleanly expands upwards against us.
		// We completely ignore Bullish Absorption floors here to let the cluster resolve naturally.
		if analytics.VolumeRank >= 6 {
			if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
				return "EXIT_SHORT"
			}
		}
	}

	return "HOLD"
}

// 3. TAKE PROFIT METHOD
func (s *MorningRankStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if netQty <= 0 {
		return false
	}
	multiplier := 1.0
	if currentSide == "SHORT" {
		multiplier = -1.0
	}

	unrealizedPnL := (state.LatestPrice - averagePrice) * float64(netQty) * multiplier
	return unrealizedPnL >= s.FixedProfitTargetINR
}

// 4. STOP LOSS METHOD
func (s *MorningRankStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if netQty <= 0 {
		return false
	}
	multiplier := 1.0
	if currentSide == "SHORT" {
		multiplier = -1.0
	}

	unrealizedPnL := (state.LatestPrice - averagePrice) * float64(netQty) * multiplier
	return unrealizedPnL <= -s.MaxLossTargetINR
}
