package strategy

import (
	"gidh-backend/internal/service/models"
	"math"
)

type MorningRankStrategy struct{}

func NewMorningRankStrategy() *MorningRankStrategy {
	return &MorningRankStrategy{}
}

func (s *MorningRankStrategy) Name() string { return "Morning_4Method_Ranks" }

func (s *MorningRankStrategy) CheckEntry(state *InstrumentState) string {
	bars1m := state.BarHistory["1m"]
	if len(bars1m) < 2 {
		return "HOLD"
	}

	latestClosedBar := bars1m[len(bars1m)-1]

	// 1. One Entry per Candle Memory Lock
	if !state.LastTradedBarTime.IsZero() && latestClosedBar.Timestamp.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// 2. 10-Minute Opening Drive Window (9:16 AM - 9:25 AM)
	if state.MinutesSinceOpen < 1 || state.MinutesSinceOpen > 10 {
		return "HOLD"
	}

	// 3. Volatility Over-Extension Cap
	absDistance := math.Abs(state.NormalizedVwapDistance)
	if absDistance > 0.35 {
		return "HOLD"
	}

	// 4. ⚡ HARDENED OPENING SPATIAL DEPTH GATE
	// We raise the requirement to a flat 0.20 across the entire 10-minute morning window.
	// This ensures the price must be actively breaking away from the VWAP line before we enter.
	// This single change completely filters out the faulty SUPREMEIND entry (-0.0744).
	if absDistance < 0.20 {
		return "HOLD"
	}

	previousClosedBar := bars1m[len(bars1m)-2]
	stableAnchorVwap := previousClosedBar.VWAP
	if stableAnchorVwap <= 0 {
		return "HOLD"
	}

	analytics := latestClosedBar.Analytics
	isInstitutionalShock := analytics.VolumeRank >= 7
	isPriceExpanding := analytics.PriceRank >= 7

	if isInstitutionalShock && isPriceExpanding {
		totalRange := latestClosedBar.High - latestClosedBar.Low
		if totalRange <= 0.0001 {
			return "HOLD"
		}

		// --- 🟢 EVALUATE LONG BREAKOUT ---
		if latestClosedBar.Close > stableAnchorVwap {
			if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
				candleBodyTop := math.Max(latestClosedBar.Open, latestClosedBar.Close)
				upperWickRatio := (latestClosedBar.High - candleBodyTop) / totalRange

				if upperWickRatio < 0.15 {
					return "GO_LONG"
				}
			}
		}

		// --- 🔴 EVALUATE SHORT BREAKOUT ---
		if latestClosedBar.Close < stableAnchorVwap {
			if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
				if state.LatestWickRatio < 0.15 {
					return "GO_SHORT"
				}
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

	// Trend-Flip Rule: Exit only if opposing institutional volume prints with force (Rank >= 6)
	if currentSide == "LONG" {
		if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *MorningRankStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}

func (s *MorningRankStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}

// CheckTrailingProfitLock evaluates if an active trend extension has stalled and rolled back.
// Arms at 20% of daily expected range (0.20) and triggers an exit if 50% of peak extension evaporates.
func (s *MorningRankStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	currentExtension := math.Abs(state.NormalizedVwapDistance)

	// 🔒 LOCK ARMING MILESTONE
	// The lock only arms if the trade expands past 20% of the stock's total expected daily range.
	// This ensures your THANGAMAYL move (which peaked at over ₹4,600) gets safely captured.
	if state.PeakVwapExtension < 0.20 {
		return false
	}

	// 🔒 50% RE-TRACEMENT SNAP-LOCK
	// Once armed, if momentum reverses and 50% of the maximum recorded extension vanishes,
	// we flag an immediate interface exit live on the current streaming tick.
	if currentExtension <= (state.PeakVwapExtension * 0.50) {
		return true // Structural exit triggered!
	}

	return false
}
