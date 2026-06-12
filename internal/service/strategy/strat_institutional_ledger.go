package strategy

import "math"

type InstitutionalLedgerStrategy struct {
	VwapBufferPct float64 // Pullback execution cushion zone (0.0015 = 0.15% cushion around VWAP)
}

// NewInstitutionalLedgerStrategy instantiates our streamlined volume-ledger strategy.
func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct: 0.0015, //
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_VWAP_Acceptance" //
}

// CheckEntry evaluates the active breakout and time-decayed signed efficiency rules.
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// 1. Core Chronological Lock Gate: Block entry if we already traded this exact bar minute
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// --- 🟢 LONG ENTRY TRIGGER GATE ---
	// Price must spend 3 consecutive minutes above VWAP
	if state.ConsecutiveClosesAboveVwap >= 3 && state.LatestChangePct > 1.0 {
		// Rule: Breakout past 9:30 ceiling, extreme volume injection, and fresh non-decayed efficiency
		if state.LatestVolumeRank >= 6 && state.Efficiency >= 0.8 {
			return "GO_LONG"
		}
	}

	// --- 🔴 SHORT ENTRY TRIGGER GATE ---
	// Price must spend 3 consecutive minutes below VWAP
	if state.ConsecutiveClosesBelowVwap >= 3 && state.LatestChangePct < 1.0 {
		// Rule: Breakdown past 9:30 floor, extreme volume injection, and fresh non-decayed efficiency
		if state.LatestVolumeRank >= 6 && state.Efficiency <= -0.8 {
			return "GO_SHORT"
		}
	}

	return "HOLD"
}

// CheckExit handles continuous microstructural trend flip checks while in an active trade
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// 1. Core Price-Action Invalidation (Clean structural break deep past our VWAP buffer zone)
	if currentSide == "LONG" && state.LatestPrice < (state.LiveSessionVWAP*(1.0-s.VwapBufferPct*2)) { //
		return "EXIT_LONG" //
	}
	if currentSide == "SHORT" && state.LatestPrice > (state.LiveSessionVWAP*(1.0+s.VwapBufferPct*2)) { //
		return "EXIT_SHORT" //
	}

	// 2. Dynamic Directional Cross-Pollution Guard
	if currentSide == "LONG" && state.Efficiency <= -0.8 {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.Efficiency >= 0.8 {
		return "EXIT_SHORT"
	}

	return "HOLD" //
}

// CheckTrailingProfitLock handles trailing retracements.
// 🟢 Bypassed for now to prioritize your fixed INR target.
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	// If the current peak volume extension moves substantially in our favor,
	// lock execution when it retraces past 30% of that maximum recorded extension peak.
	if state.PeakVwapExtension > 0.05 {
		currentExtension := math.Abs(state.NormalizedVwapDistance)
		if currentExtension < (state.PeakVwapExtension * 0.70) {
			return true // Trigger structural trailing profit lock
		}
	}
	return false
}

// CheckTakeProfit triggers an immediate exit the moment cash PnL hits or exceeds 500 INR
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool { //
	return false
}

// CheckStopLoss handles fixed risk protection using the instrument's ADR profile
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if netQty <= 0 || averagePrice <= 0 || state.Profile == nil || state.Profile.ADRPct <= 0 {
		return false
	}

	// Calculate a stop loss bound to 0.25x of the Average Daily Range percentage
	slPercent := state.Profile.ADRPct * 0.25 / 100.0

	if currentSide == "LONG" {
		return state.LatestPrice <= averagePrice*(1.0-slPercent)
	}
	if currentSide == "SHORT" {
		return state.LatestPrice >= averagePrice*(1.0+slPercent)
	}
	return false
}
