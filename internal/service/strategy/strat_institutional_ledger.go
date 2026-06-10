package strategy

import (
	"math"
)

type InstitutionalLedgerStrategy struct {
	AdrScaleMultiplier float64 // 0.05 = Pullback envelope is 5% of the asset's total ADRPct
	WipeoutThreshold   float64 // 0.60 = Exit if counter-volume hits 60% of setup volume
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		AdrScaleMultiplier: 0.05,
		WipeoutThreshold:   0.60,
	}
}

func (s *InstitutionalLedgerStrategy) Name() string { return "Institutional_Ledger_VWAP_Acceptance" }

// CheckEntry evaluates entry signals when position structure is completely FLAT
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Structural Gate: Wait for 3 continuous closes to establish a baseline trend floor
	if !state.IsVwapAcceptanceConfirmed {
		return "HOLD"
	}

	// --- 🟢 LONG STRATEGY TRACK (Gap Up Sessions) ---
	if state.IsGapUp && state.ConsecutiveClosesAboveVwap > 0 {
		if state.BullishPushVolume > 0 && (state.BearishPushVolume/state.BullishPushVolume) < 0.30 {
			if state.LatestPrice >= state.LiveSessionVWAP {
				return "GO_LONG" // 🔥 Fires immediately now instead of staging
			}
		}
	}

	// --- 🔴 SHORT STRATEGY TRACK (Gap Down Sessions) ---
	if state.IsGapDown && state.ConsecutiveClosesBelowVwap > 0 {
		if state.BearishPushVolume > 0 && (state.BullishPushVolume/state.BearishPushVolume) < 0.30 {
			if state.LatestPrice <= state.LiveSessionVWAP {
				return "GO_SHORT" // 🔥 Fires immediately now instead of staging
			}
		}
	}

	return "HOLD"
}

// CheckExit handles continuous microstructural trend flip checks while in an active trade
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// Volume Effectiveness Balance Sheet Protection Check
	if currentSide == "LONG" && state.BullishPushVolume > 0 {
		if (state.BearishPushVolume / state.BullishPushVolume) >= s.WipeoutThreshold {
			return "EXIT_LONG"
		}
	}
	if currentSide == "SHORT" && state.BearishPushVolume > 0 {
		if (state.BullishPushVolume / state.BearishPushVolume) >= s.WipeoutThreshold {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// CheckTrailingProfitLock performs intelligent volatility retracement tracking
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	currentExtension := math.Abs(state.NormalizedVwapDistance)

	if state.PeakVwapExtension < 0.20 {
		return false
	}

	if currentSide == "LONG" && state.BullishPushVolume > 0 {
		if (state.BearishPushVolume / state.BullishPushVolume) < 0.15 {
			return currentExtension <= (state.PeakVwapExtension * 0.30)
		}
	}
	if currentSide == "SHORT" && state.BearishPushVolume > 0 {
		if (state.BullishPushVolume / state.BearishPushVolume) < 0.15 {
			return currentExtension <= (state.PeakVwapExtension * 0.30)
		}
	}

	return currentExtension <= (state.PeakVwapExtension * 0.60)
}

func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
