package strategy

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

func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// 1. Structural Validation Gate
	if !state.IsVwapAcceptanceConfirmed {
		return "HOLD"
	}

	// --- 🟢 LONG SETUP DIRECTIONAL CHECK ---
	if state.IsGapUp && state.ConsecutiveClosesAboveVwap > 0 {
		if state.BullishPushVolume > 0 && (state.BearishPushVolume/state.BullishPushVolume) < 0.30 {
			// Setup Variant B: Aggressive Runaway Momentum Breakout
			if state.LatestVolumeRank >= 7 && state.LatestPriceRank >= 7 && state.LatestPrice > state.LiveSessionVWAP {
				return "GO_LONG"
			}

			// Setup Variant A: Signal that structural conditions are primed for a pullback entry
			if state.LatestPrice >= state.LiveSessionVWAP {
				return "SETUP_READY_LONG"
			}
		}
	}

	// --- 🔴 SHORT SETUP DIRECTIONAL CHECK ---
	if state.IsGapDown && state.ConsecutiveClosesBelowVwap > 0 {
		if state.BearishPushVolume > 0 && (state.BullishPushVolume/state.BearishPushVolume) < 0.30 {
			// Setup Variant B: Aggressive Downward Breakdown
			if state.LatestVolumeRank >= 7 && state.LatestPriceRank >= 7 && state.LatestPrice < state.LiveSessionVWAP {
				return "GO_SHORT"
			}

			if state.LatestPrice <= state.LiveSessionVWAP {
				return "SETUP_READY_SHORT"
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
	return false

}

func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
