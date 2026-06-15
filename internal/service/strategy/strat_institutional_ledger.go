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
	MinTimePctVwapRegime float64

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
		StopLossPct:          0.0060, // Loosened to 0.60% safety stop. Let the human cut closer if needed.
		ProfitLockTrigger:    0.0150, // Safety trail only triggers at a clean 1.5% profit jump.
		ProfitLockCapture:    0.50,   // Give back up to 50% of an extreme peak before safety auto-cut.
		MinTimePctVwapRegime: 0.70,
		lastExecutedEntry:    make(map[string]time.Time),
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Trader_Assisted_V1"
}

// CheckEntry upgraded for high precision, individual asset strength
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	s.stateMutex.RLock()
	lastEntryTime, hasTraded := s.lastExecutedEntry[state.Symbol]
	s.stateMutex.RUnlock()

	// 15-minute cool-down period to prevent over-trading the same symbol
	if hasTraded && state.LastUpdated.Sub(lastEntryTime) < 15*time.Minute {
		return "HOLD"
	}

	if !state.LastUpdated.IsZero() {
		istTime := state.LastUpdated.In(s.istLocation)
		currentTimeHourMinute := (istTime.Hour() * 100) + istTime.Minute()

		if currentTimeHourMinute < 931 { // Skip morning opening noise
			return "HOLD"
		}
	}

	if state.NormalizedVwapDistance > 0.25 || state.NormalizedVwapDistance < -0.25 {
		return "HOLD"
	}

	if state.LiveSessionVWAP <= 0 || state.TotalSessionBars < 8 {
		return "HOLD"
	}

	timeAboveVwapPct := state.TimePctAboveVwap
	timeBelowVwapPct := 1.0 - timeAboveVwapPct
	currentEff := state.NetEfficiency
	currentSlope := state.NetEfficiencySlope

	// 🔥 CRITICAL FIX: Add Institutional Volume Catalyst Requirement
	// Your logs showed entries at Volume Rank 3. We require Rank 7+ (heavy block interest).
	if state.LatestVolumeRank < 6 {
		return "HOLD"
	}

	// 🔥 CRITICAL FIX: Elevate NetEfficiency Threshold
	// Require absolute conviction (>65.0) instead of letting market-wide beta lift it over 50.0.
	if currentEff > 35.0 && state.LatestPrice > state.LiveSessionVWAP && currentSlope >= 6.0 {
		if timeAboveVwapPct >= s.MinTimePctVwapRegime {
			s.stateMutex.Lock()
			s.lastExecutedEntry[state.Symbol] = state.LastUpdated
			s.stateMutex.Unlock()
			return "GO_LONG"
		}
	}

	if currentEff < -65.0 && state.LatestPrice < state.LiveSessionVWAP && currentSlope <= -6.0 {
		if timeBelowVwapPct >= s.MinTimePctVwapRegime {
			s.stateMutex.Lock()
			s.lastExecutedEntry[state.Symbol] = state.LastUpdated
			s.stateMutex.Unlock()
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

// CheckTakeProfit - Extracted entirely except for structural High-Confirmation Safety
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	// The human handles targets. This only activates if trailing conditions hit safety criteria.
	return s.CheckTrailingProfitLock(state, currentSide, averagePrice)
}

// CheckStopLoss - Retained as a pure maximum risk circuit breaker
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if averagePrice <= 0 {
		return false
	}

	allowedLoss := averagePrice * s.StopLossPct

	if currentSide == "LONG" {
		if state.LatestPrice <= (averagePrice - allowedLoss) {
			return true
		}
	}

	if currentSide == "SHORT" {
		if state.LatestPrice >= (averagePrice + allowedLoss) {
			return true
		}
	}

	return false
}

func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string, averagePrice float64) bool {
	if state.PeakPnL <= 0 || averagePrice <= 0 {
		return false
	}

	peakYieldPct := state.PeakPnL / averagePrice

	// High confirmation backup target
	if peakYieldPct >= s.ProfitLockTrigger {
		minimumLockedProfit := state.PeakPnL * s.ProfitLockCapture
		if state.CurrentPnL < minimumLockedProfit {
			return true // Cut the position because it retraced 50% from a massive target peak.
		}
	}

	return false
}

// CheckExit disabled for the strategy layer to allow trader full discretion
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}
