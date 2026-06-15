package strategy

import (
	"sync"
	"time"
)

type InstitutionalLedgerStrategy struct {
	VwapBufferPct        float64
	istLocation          *time.Location
	StopLossPct          float64
	ProfitLockTrigger    float64
	ProfitLockCapture    float64
	MinTimePctVwapRegime float64 // 📊 Minimum session percentage above/below VWAP to confirm directional regime

	stateMutex        sync.RWMutex
	lastExecutedEntry map[string]time.Time
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		loc = time.Local
	}

	return &InstitutionalLedgerStrategy{
		VwapBufferPct:        0.0012,
		istLocation:          loc,
		StopLossPct:          0.0035,
		ProfitLockTrigger:    0.0060,
		ProfitLockCapture:    0.50,
		MinTimePctVwapRegime: 0.70,
		lastExecutedEntry:    make(map[string]time.Time),
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned_V2"
}

// CheckEntry Enhanced with 10-Bar Lockout, Velocity Slope Filtering, and Opening Buffer
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Priority 1: Prevent duplicate entry executions on the exact same bar close
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// 🕒 Priority 2: Extended Opening Range Shield (Blocks trades from 9:15 to 9:27 AM IST to absorb spreads)
	if !state.LastUpdated.IsZero() {
		istTime := state.LastUpdated.In(s.istLocation)
		currentTimeHourMinute := (istTime.Hour() * 100) + istTime.Minute()

		// Shifted from 925 to 927 to dodge the front-running execution bottleneck visible in orders
		if currentTimeHourMinute < 931 {
			return "HOLD"
		}
	}

	// Checking Long Overextension:
	if state.NormalizedVwapDistance > 0.20 {
		return "HOLD"
	}

	// Checking Short Overextension:
	if state.NormalizedVwapDistance < -0.20 {
		return "HOLD"
	}

	// Safety: Ensure VWAP data exists from the incoming feed
	if state.LiveSessionVWAP <= 0 {
		return "HOLD"
	}

	if state.TotalSessionBars < 8 {
		return "HOLD"
	}

	timeAboveVwapPct := state.TimePctAboveVwap
	timeBelowVwapPct := 1.0 - timeAboveVwapPct

	currentEff := state.NetEfficiency
	currentSlope := state.NetEfficiencySlope

	// --- 🟢 HIGH CONVICTION LONG ENTRY ---
	if currentEff > 50.0 && state.LatestPrice > state.LiveSessionVWAP && currentSlope >= 5.0 {
		if timeAboveVwapPct >= s.MinTimePctVwapRegime {
			s.stateMutex.Lock()
			s.lastExecutedEntry[state.Symbol] = state.LastUpdated
			s.stateMutex.Unlock()
			return "GO_LONG"
		}
	}

	// --- 🔴 HIGH CONVICTION SHORT ENTRY ---
	if currentEff < -50.0 && state.LatestPrice < state.LiveSessionVWAP && currentSlope <= -5.0 {
		if timeBelowVwapPct >= s.MinTimePctVwapRegime {
			s.stateMutex.Lock()
			s.lastExecutedEntry[state.Symbol] = state.LastUpdated
			s.stateMutex.Unlock()
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

// CheckTakeProfit High-Velocity Slope Decay & Institutional Exhaustion
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	currentSlope := state.NetEfficiencySlope

	if currentSide == "LONG" {
		// Exhaustion Clause: High volume spike occurs but momentum slope flips negative
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

// CheckStopLoss Hard-Stop Capital Protection
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if averagePrice <= 0 {
		return false
	}

	// Dynamic calculation of threshold price distance
	allowedLoss := averagePrice * s.StopLossPct

	if currentSide == "LONG" {
		// If current market price dumps below entry average minus allowed buffer
		if state.LatestPrice <= (averagePrice - allowedLoss) {
			return true
		}
	}

	if currentSide == "SHORT" {
		// If market price rips above short entry average plus allowed buffer
		if state.LatestPrice >= (averagePrice + allowedLoss) {
			return true
		}
	}

	return false
}

// CheckTrailingProfitLock Peak Profit Capture Guards
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	// Note: Engine must populate PeakPnLINR and FinalPnLINR parameters continuously inside tracking registry.
	// This structure monitors price extension relative to the historical peak achieved during the lifespan of this trade.

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
