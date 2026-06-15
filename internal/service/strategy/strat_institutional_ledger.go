package strategy

import "math"

type InstitutionalLedgerStrategy struct {
	VwapBufferPct float64
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct: 0.0012,
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned"
}

// CheckEntry Enhanced with 10-Bar Lockout and Velocity Slope Filtering
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Priority 1: Prevent duplicate entry executions on the exact same bar close
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// 🕒 Priority 2: 10-Bar Opening Range Shield (Blocks trades from 9:15 to 9:25)
	if state.TotalSessionBars < 10 {
		return "HOLD"
	}

	// 🛡️ ACCELERATION REJECT GATEWAY: Prevents buying overextended tops
	// If distance is > 0.15 (Long) or < -0.15 (Short), do not chase the move.
	if math.Abs(state.NormalizedVwapDistance) > 0.15 {
		return "HOLD"
	}

	// Safety: Ensure VWAP data exists from the incoming feed
	if state.LiveSessionVWAP <= 0 {
		return "HOLD"
	}

	currentEff := state.NetEfficiency
	currentSlope := state.NetEfficiencySlope

	// --- 🟢 HIGH CONVICTION LONG ENTRY ---
	// 1. Efficiency absolute threshold is met (> 35)
	// 2. Price is trading cleanly above VWAP
	// 3. 🔥 NEW: Slope confirms explosive institutional acceleration (>= 2.0)
	if currentEff > 35.0 && state.LatestPrice > state.LiveSessionVWAP && currentSlope >= 2.0 {
		return "GO_LONG"
	}

	// --- 🔴 HIGH CONVICTION SHORT ENTRY ---
	// 1. Efficiency absolute threshold is met (< -35)
	// 2. Price is trading cleanly below VWAP
	// 3. 🔥 NEW: Slope confirms explosive downward liquidation (<= -2.0)
	if currentEff < -35.0 && state.LatestPrice < state.LiveSessionVWAP && currentSlope <= -2.0 {
		return "GO_SHORT"
	}

	return "HOLD"
}

// CheckTakeProfit High-Velocity Slope Decay
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	currentSlope := state.NetEfficiencySlope

	if currentSide == "LONG" {
		// Exhaustion Clause: P90/P97 Volume spike occurs but momentum slope flips negative
		if state.LatestVolumeRank >= 6 && currentSlope < 0 {
			return true
		}

		// Trailing De-acceleration Clause: Fast trend breakdown guard
		if currentSlope < -2.0 {
			return true
		}
	}

	if currentSide == "SHORT" {
		// Mirror logic for short positions
		if state.LatestVolumeRank >= 6 && currentSlope > 0 {
			return true
		}

		if currentSlope > 2.0 {
			return true
		}
	}

	return false
}

// CheckExit Trend Reversal Protection
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	currentSlope := state.NetEfficiencySlope

	if currentSide == "LONG" {
		if currentSlope < -0.5 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if currentSlope > 0.5 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return false
}

func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
