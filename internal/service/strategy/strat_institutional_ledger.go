package strategy

import (
	"math"
)

type InstitutionalLedgerStrategy struct {
	VwapBufferPct float64
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct: 0.0012, // Baseline value buffer
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned"
}

func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Priority 1: Prevent duplicate/back-to-back execution on the exact same bar
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) { //
		return "HOLD" //
	}

	historyLength := len(state.NetEfficiencyHistory) //
	if historyLength < 4 {                           //
		return "HOLD" //
	}

	// --- 🛠️ PRIORITY 4: VWAP DISTANCE STRETCH FILTER ---
	// Prevent chasing moves that are already horizontally overextended (> 0.25)
	if math.Abs(state.NormalizedVwapDistance) > 0.25 {
		return "HOLD"
	}

	currentEff := state.NetEfficiency                                 //
	previousEff := state.NetEfficiencyHistory[historyLength-2]        //
	trailing3AvgEff := (state.NetEfficiencyHistory[historyLength-4] + //
		state.NetEfficiencyHistory[historyLength-3] + //
		previousEff) / 3.0 //

	// --- 🟢 VERSION 1 HIGH-CONVICTION LONG TRIGGER ---
	if state.LatestVolumeRank >= 6 { //
		if currentEff > 50.0 { //
			if trailing3AvgEff > 0.0 { //
				effDelta := currentEff - previousEff //
				if effDelta > 15.0 {                 //
					if state.NormalizedVwapDistance > 0 { //
						if state.TimePctAboveVwap > 0.35 { //
							return "GO_LONG" //
						}
					}
				}
			}
		}
	}

	// --- 🔴 VERSION 1 HIGH-CONVICTION SHORT TRIGGER ---
	if state.LatestVolumeRank >= 6 { //
		if currentEff < -50.0 { //
			if trailing3AvgEff < 0.0 { //
				effDelta := currentEff - previousEff //
				if effDelta < -15.0 {                //
					if state.NormalizedVwapDistance < 0 { //
						if state.TimePctAboveVwap < 0.25 { //
							return "GO_SHORT" //
						}
					}
				}
			}
		}
	}

	return "HOLD" //
}

// CheckExit now exclusively handles structural trend breaks and extensions
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	dynamicCushion := s.VwapBufferPct                       //
	if state.Profile != nil && state.Profile.ADRPct > 4.0 { //
		dynamicCushion = s.VwapBufferPct * 1.5 //
	}

	if currentSide == "LONG" {
		// Exit Pillar 1: Pure Trend Failure Invalidation
		if state.LatestPrice < (state.LiveSessionVWAP * (1.0 - dynamicCushion)) { //
			return "EXIT_LONG" //
		}
		// Climax Extension
		if state.NormalizedVwapDistance > 2.8 { //
			return "EXIT_LONG" //
		}
	}

	if currentSide == "SHORT" {
		// Exit Pillar 1: Pure Trend Failure Invalidation
		if state.LatestPrice > (state.LiveSessionVWAP * (1.0 + dynamicCushion)) { //
			return "EXIT_SHORT" //
		}
		// Climax Extension
		if state.NormalizedVwapDistance < -2.8 { //
			return "EXIT_SHORT" //
		}
	}

	return "HOLD" //
}

// CheckTakeProfit now explicitly handles your deceleration and profit protection mechanics
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	currentEff := state.NetEfficiency

	if currentSide == "LONG" {
		// --- 🛠️ PRIORITY 2: EFFICIENCY ACCELERATION RETRACEMENT (25-30%) ---
		// Capture institutional deceleration before it round-trips
		if state.PeakEfficiency > 50.0 { //
			decayThreshold := state.PeakEfficiency * 0.75 // Retraces 25% (Preserves 75%)
			if currentEff < decayThreshold {
				return true // Trigger Take Profit
			}
		}

		// --- 🛠️ PRIORITY 3: PROFIT PROTECTION (50% Peak PnL Giveback) ---
		if state.PeakPnL > 0 && state.CurrentPnL < (state.PeakPnL*0.50) {
			return true // Trigger Take Profit Protection
		}
	}

	if currentSide == "SHORT" {
		// --- 🛠️ PRIORITY 2: EFFICIENCY ACCELERATION RETRACEMENT (SHORTS) ---
		if state.PeakEfficiency > 50.0 { //
			decayThreshold := state.PeakEfficiency * 0.75 // Mirror logic for short velocity
			if currentEff > -decayThreshold {
				return true // Trigger Take Profit
			}
		}

		// --- 🛠️ PRIORITY 3: PROFIT PROTECTION (SHORTS) ---
		if state.PeakPnL > 0 && state.CurrentPnL < (state.PeakPnL*0.50) {
			return true // Trigger Take Profit Protection
		}
	}

	return false
}

func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return false //
}

func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false //
}
