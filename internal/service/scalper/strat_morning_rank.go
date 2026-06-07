package scalper

import (
	"gidh-backend/internal/service/models"
)

type MorningRankStrategy struct {
	Timeframe            string
	FixedProfitTargetINR float64
	MaxLossTargetINR     float64
}

func NewMorningRankStrategy() *MorningRankStrategy {
	return &MorningRankStrategy{
		Timeframe:            "1m",
		FixedProfitTargetINR: 2000.0, // Hardcoded cash target goal
		MaxLossTargetINR:     600.0,  // Risk barrier cap
	}
}

func (s *MorningRankStrategy) Name() string { return "Morning_4Method_Ranks" }

func (s *MorningRankStrategy) CheckEntry(state *InstrumentState) string {
	bars := state.BarHistory[s.Timeframe]
	if len(bars) == 0 || state.LiveSessionVWAP <= 0.0 {
		return "HOLD"
	}

	latestClosedBar := bars[len(bars)-1]
	analytics := latestClosedBar.Analytics

	if analytics.VolumeRank == 7 {

		// Evaluate using 5% of Daily ATR as our VWAP Band channel width
		bandState := evaluateVWAPBand(state, 0.05)

		switch bandState {
		case "ABOVE_BAND":
			if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
				return "GO_LONG"
			}
		case "BELOW_BAND":
			if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
				return "GO_SHORT"
			}
		}
	}

	return "HOLD"
}

func (s *MorningRankStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	bars := state.BarHistory[s.Timeframe]
	if len(bars) == 0 {
		return "HOLD"
	}

	latestClosedBar := bars[len(bars)-1]
	analytics := latestClosedBar.Analytics

	// 1. General Low-Volume Momentum Drop: Exit if absolute session-wise activity dries up completely
	if analytics.VolumeRank <= 4 {
		return "EXIT_" + currentSide
	}

	// 2. Institutional Counter-Trend Exit: Only exit if the opposing move is backed by heavy size (p90+)
	if currentSide == "LONG" && analytics.VolumeRank >= 6 &&
		(analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish) {
		return "EXIT_LONG"
	}

	if currentSide == "SHORT" && analytics.VolumeRank >= 6 &&
		(analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish) {
		return "EXIT_SHORT"
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
