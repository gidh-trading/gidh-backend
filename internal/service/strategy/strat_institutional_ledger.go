package strategy

import (
	"math"
)

type InstitutionalLedgerStrategy struct {
	VwapBufferPct    float64 // Pullback execution envelope (0.0015 = 0.15% cushion zone around VWAP)
	WipeoutThreshold float64 // Critical volume balance cutoff (0.60 = Exit if counter-volume hits 60% of setup volume)
}

// NewInstitutionalLedgerStrategy instantiates our professional institutional volume-ledger strategy.
func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct:    0.0015,
		WipeoutThreshold: 0.60,
	}
}

func (s *InstitutionalLedgerStrategy) Name() string { return "Institutional_Ledger_VWAP_Acceptance" }

// CheckEntry evaluates entry signals when position structure is completely FLAT
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Refuse to interact until sustained time-at-price over or under the VWAP anchor is locked
	if !state.IsVwapAcceptanceConfirmed {
		return "HOLD"
	}

	distFromVwap := math.Abs(state.LatestPrice - state.LiveSessionVWAP)
	allowedTriggerZone := state.LiveSessionVWAP * s.VwapBufferPct

	// --- 🟢 LONG STRATEGY TRACK (Gap Up Filter Active) ---
	if state.IsGapUp && state.ConsecutiveClosesAboveVwap > 0 {
		if state.BullishPushVolume > 0 {
			counterForceRatio := state.BearishPushVolume / state.BullishPushVolume

			// Establish entry guard: Opposing counter-force volume must be less than 30% of push volume
			if counterForceRatio < 0.30 {

				// Setup A: Patient Pullback directly into the VWAP support zone on thin volume
				if distFromVwap <= allowedTriggerZone && state.LatestPrice >= state.LiveSessionVWAP {
					if state.LatestVolumeRank <= 4 {
						return "GO_LONG"
					}
				}

				// Setup B: Runaway Momentum Protection (For 2% aggressive starts scaling straight up to 6%)
				if state.LatestVolumeRank >= 6 && state.LatestPriceRank >= 5 && state.LatestPrice > state.LiveSessionVWAP {
					return "GO_LONG"
				}
			}
		}
	}

	// --- 🔴 SHORT STRATEGY TRACK (Gap Down Filter Active) ---
	if state.IsGapDown && state.ConsecutiveClosesBelowVwap > 0 {
		if state.BearishPushVolume > 0 {
			counterForceRatio := state.BullishPushVolume / state.BearishPushVolume

			if counterForceRatio < 0.30 {

				// Setup A: Patient Pullback up into the underbelly of VWAP resistance on thin volume
				if distFromVwap <= allowedTriggerZone && state.LatestPrice <= state.LiveSessionVWAP {
					if state.LatestVolumeRank <= 4 {
						return "GO_SHORT"
					}
				}

				// Setup B: Runaway Downward Breakdown Protection
				if state.LatestVolumeRank >= 6 && state.LatestPriceRank >= 5 && state.LatestPrice < state.LiveSessionVWAP {
					return "GO_SHORT"
				}
			}
		}
	}

	return "HOLD"
}

// CheckExit handles continuous microstructural trend flip checks while in an active trade
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// 1. Core Price-Action Invalidation (Clean break deep past our buffer zone)
	if currentSide == "LONG" && state.LatestPrice < (state.LiveSessionVWAP*(1.0-s.VwapBufferPct*2)) {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.LatestPrice > (state.LiveSessionVWAP*(1.0+s.VwapBufferPct*2)) {
		return "EXIT_SHORT"
	}

	// 2. 🥊 Volume Effectiveness Balance Sheet Protection
	// If the opposing team steps in and commits raw volume that eclipses our threshold, exit immediately
	if currentSide == "LONG" && state.BullishPushVolume > 0 {
		distributionRatio := state.BearishPushVolume / state.BullishPushVolume
		if distributionRatio >= s.WipeoutThreshold {
			return "EXIT_LONG" // Original institutional buyers are overwhelmed or distributing out
		}
	}

	if currentSide == "SHORT" && state.BearishPushVolume > 0 {
		accumulationRatio := state.BullishPushVolume / state.BearishPushVolume
		if accumulationRatio >= s.WipeoutThreshold {
			return "EXIT_SHORT" // Shorts are actively being squeezed out by massive buyer absorption
		}
	}

	return "HOLD"
}

// CheckTrailingProfitLock performs intelligent volatility retracement tracking
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	currentExtension := math.Abs(state.NormalizedVwapDistance)

	// Lock arms only if the trade expands past 20% of its expected daily range boundary
	if state.PeakVwapExtension < 0.20 {
		return false
	}

	// Dynamic leash configuration based on ledger domination metrics
	if currentSide == "LONG" && state.BullishPushVolume > 0 {
		sellingRatio := state.BearishPushVolume / state.BullishPushVolume

		if sellingRatio < 0.15 {
			// Dominant buyers completely dictate order flow. Give the asset huge breathing space
			// to surf through natural mid-day consolidation dips. Trail out only if 70% of extension drops.
			return currentExtension <= (state.PeakVwapExtension * 0.30)
		}
	}

	if currentSide == "SHORT" && state.BearishPushVolume > 0 {
		buyingRatio := state.BullishPushVolume / state.BearishPushVolume
		if buyingRatio < 0.15 {
			return currentExtension <= (state.PeakVwapExtension * 0.30)
		}
	}

	// Standard fallback trailing leash if the ledger balance sheet is closely fought (exit if 40% drops)
	return currentExtension <= (state.PeakVwapExtension * 0.60)
}

func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
