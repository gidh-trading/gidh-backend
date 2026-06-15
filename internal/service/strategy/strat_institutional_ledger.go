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
		StopLossPct:          0.0045, // Loosened stop loss slightly to 0.45% to survive noise
		ProfitLockTrigger:    0.0075, // Trigger trail at 0.75% profit
		ProfitLockCapture:    0.60,   // Lock in 60% of peak
		MinTimePctVwapRegime: 0.70,
		lastExecutedEntry:    make(map[string]time.Time),
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned_V3"
}

// CheckEntry Enhanced with Cooldowns and Velocity slope filtering
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// ✅ FIX: Cooldown Timer. Prevent re-entering the same stock within 15 minutes of the last trade
	s.stateMutex.RLock()
	lastEntryTime, hasTraded := s.lastExecutedEntry[state.Symbol]
	s.stateMutex.RUnlock()

	if hasTraded && state.LastUpdated.Sub(lastEntryTime) < 15*time.Minute {
		return "HOLD"
	}

	if !state.LastUpdated.IsZero() {
		istTime := state.LastUpdated.In(s.istLocation)
		currentTimeHourMinute := (istTime.Hour() * 100) + istTime.Minute()

		if currentTimeHourMinute < 931 {
			return "HOLD"
		}
	}

	if state.NormalizedVwapDistance > 0.20 || state.NormalizedVwapDistance < -0.20 {
		return "HOLD"
	}

	if state.LiveSessionVWAP <= 0 || state.TotalSessionBars < 8 {
		return "HOLD"
	}

	timeAboveVwapPct := state.TimePctAboveVwap
	timeBelowVwapPct := 1.0 - timeAboveVwapPct
	currentEff := state.NetEfficiency
	currentSlope := state.NetEfficiencySlope

	if currentEff > 50.0 && state.LatestPrice > state.LiveSessionVWAP && currentSlope >= 5.0 {
		if timeAboveVwapPct >= s.MinTimePctVwapRegime {
			s.stateMutex.Lock()
			s.lastExecutedEntry[state.Symbol] = state.LastUpdated
			s.stateMutex.Unlock()
			return "GO_LONG"
		}
	}

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

// CheckTakeProfit Exhaustion & Trailing Profit Lock
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	currentSlope := state.NetEfficiencySlope

	if s.CheckTrailingProfitLock(state, currentSide, averagePrice) {
		return true
	}

	// ✅ FIX: Ensure Exhaustion / Deceleration exits ONLY trigger if the trade is actually in profit
	if state.CurrentPnL > 0 {
		if currentSide == "LONG" {
			if state.LatestVolumeRank >= 6 && currentSlope < 0 {
				return true
			}
			if currentSlope < -2.5 { // Loosened from -2.0
				return true
			}
		}

		if currentSide == "SHORT" {
			if state.LatestVolumeRank >= 6 && currentSlope > 0 {
				return true
			}
			if currentSlope > 2.5 { // Loosened from 2.0
				return true
			}
		}
	}

	return false
}

// CheckStopLoss Hard-Stop Capital Protection
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

	if peakYieldPct >= s.ProfitLockTrigger {
		minimumLockedProfit := state.PeakPnL * s.ProfitLockCapture
		if state.CurrentPnL < minimumLockedProfit {
			return true
		}
	}

	return false
}

// CheckExit Trend Reversal Protection
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	currentSlope := state.NetEfficiencySlope

	// ✅ FIX: Loosened structural exit thresholds from 0.5 to 1.5.
	// This prevents the engine from shaking you out during 1-minute micro-pullbacks.
	if currentSide == "LONG" {
		if currentSlope < -1.5 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if currentSlope > 1.5 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}
