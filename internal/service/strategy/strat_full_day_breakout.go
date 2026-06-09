package strategy

import (
	"gidh-backend/internal/service/models"
	"math"
)

// FullDayBreakoutStrategy monitors the entire session for macro institutional breakout continuations.
type FullDayBreakoutStrategy struct{}

func NewFullDayBreakoutStrategy() *FullDayBreakoutStrategy {
	return &FullDayBreakoutStrategy{}
}

func (s *FullDayBreakoutStrategy) Name() string { return "Full_Day_Macro_Trend_Continuity" }

func (s *FullDayBreakoutStrategy) CheckEntry(state *InstrumentState) string {
	bars1m := state.BarHistory["1m"]
	if len(bars1m) < 2 {
		return "HOLD"
	}

	latestClosedBar := bars1m[len(bars1m)-1]

	// 1. One Entry per Candle Memory Lock
	if !state.LastTradedBarTime.IsZero() && latestClosedBar.Timestamp.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// 2. Core Session Constraints (9:16 AM to 3:15 PM IST)
	if state.MinutesSinceOpen < 1 || state.MinutesSinceOpen >= 360 {
		return "HOLD"
	}

	// 3. 🛑 DYNAMIC VOLATILITY OVER-EXTENSION CAP
	absDistance := math.Abs(state.NormalizedVwapDistance)
	var maximumAllowedExtension float64 = 0.65
	if state.MinutesSinceOpen <= 30 {
		maximumAllowedExtension = 0.35
	}

	if absDistance > maximumAllowedExtension {
		return "HOLD"
	}

	// 4. Dynamic Opening Spatial Depth Filter
	var minimumRequiredStretch float64 = 0.05
	if state.MinutesSinceOpen <= 3 {
		minimumRequiredStretch = 0.20
	}
	if absDistance < minimumRequiredStretch {
		return "HOLD"
	}

	previousClosedBar := bars1m[len(bars1m)-2]
	stableAnchorVwap := previousClosedBar.VWAP
	if stableAnchorVwap <= 0 {
		return "HOLD"
	}

	analytics := latestClosedBar.Analytics

	// Core Volume Check: Volume > P97 and Price > P97
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

func (s *FullDayBreakoutStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	bars1m := state.BarHistory["1m"]
	if len(bars1m) == 0 {
		return "HOLD"
	}

	latestClosedBar := bars1m[len(bars1m)-1]
	analytics := latestClosedBar.Analytics

	// Trend-Flip Exit Rule
	if currentSide == "LONG" {
		if analytics.VolumeRank >= 6 && (analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish) {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if analytics.VolumeRank >= 6 && (analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish) {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *FullDayBreakoutStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}

func (s *FullDayBreakoutStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
