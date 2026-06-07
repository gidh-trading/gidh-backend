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
		FixedProfitTargetINR: 1000.0, // Hardcoded cash target goal
		MaxLossTargetINR:     2500.0, // Risk barrier cap
	}
}

func (s *MorningRankStrategy) Name() string { return "Morning_4Method_Ranks" }

// 1. ENTRY METHOD
func (s *MorningRankStrategy) CheckEntry(state *InstrumentState) string {
	bars := state.BarHistory[s.Timeframe]
	if len(bars) == 0 || state.LiveSessionVWAP <= 0.0 {
		return "HOLD"
	}

	latestClosedBar := bars[len(bars)-1]
	analytics := latestClosedBar.Analytics

	if analytics.VolumeRank == 7 {
		if state.LatestPrice > state.LiveSessionVWAP {
			if analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish {
				return "GO_LONG"
			}
		}
		if state.LatestPrice < state.LiveSessionVWAP {
			if analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish {
				return "GO_SHORT"
			}
		}
	}
	return "HOLD"
}

// 2. TECHNICAL EXIT METHOD
func (s *MorningRankStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	bars := state.BarHistory[s.Timeframe]
	if len(bars) == 0 {
		return "HOLD"
	}

	latestClosedBar := bars[len(bars)-1]
	analytics := latestClosedBar.Analytics

	if analytics.VolumeRank <= 3 {
		return "EXIT_" + currentSide
	}

	if currentSide == "LONG" && (analytics.Direction == models.DirBearish || analytics.Direction == models.DirStrongBearish) {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && (analytics.Direction == models.DirBullish || analytics.Direction == models.DirStrongBullish) {
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
