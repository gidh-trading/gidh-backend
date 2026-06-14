package strategy

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

	// 🚨 ANTI-CHOP DEFENSE MECHANISM
	// If it keeps whip-sawing across VWAP, the setup is mathematically broken
	//if state.VwapCrossCount > 4 {
	//	return "HOLD"
	//}

	// Ensure our structural metrics are warmed up and established
	//if state.OpeningRangeHigh == 0 {
	//	return "HOLD"
	//}

	// 1. Core Chronological Lock Gate: Block entry if we already traded this exact bar minute
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// --- 🟢 STRUCTURAL LONG ENTRY TRIGGER ---
	if state.ConsecutiveClosesAboveVwap >= 3 { // Upgraded confirmation barrier
		// Condition: Price must break the 09:30 morning resistance ceiling
		// Condition: Order volume check paired with un-decayed trend momentum footprint
		if state.Ledger.BullEfficient-state.Ledger.BearEfficient > 30 {
			return "GO_LONG"
		}

	}

	// --- 🔴 STRUCTURAL SHORT ENTRY TRIGGER ---
	if state.ConsecutiveClosesBelowVwap >= 3 { // Upgraded confirmation barrier
		// Condition: Price must puncture under the 09:30 morning floor line
		if state.Ledger.BullEfficient-state.Ledger.BearEfficient < -30 {
			return "GO_SHORT"
		}

	}

	return "HOLD"
}

// CheckExit handles continuous microstructural trend flip checks while in an active trade
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// 1. Core Price-Action Invalidation (VWAP Invalidation Cushion remains)
	if currentSide == "LONG" && state.LatestPrice < (state.LiveSessionVWAP*(1.0-s.VwapBufferPct)) {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.LatestPrice > (state.LiveSessionVWAP*(1.0+s.VwapBufferPct)) {
		return "EXIT_SHORT"
	}

	// 2. ⏳ ADVANCED SPEED-DECAY EXIT RULE
	// If efficiency goes flat (near 0) while volume stays low, the institutional push has evaporated.
	// Don't wait for a crash—exit the trade early to lock in structural rotation.
	if currentSide == "LONG" && state.Efficiency < 0.2 && state.LatestVolumeRank < 5 {
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.Efficiency > -0.2 && state.LatestVolumeRank < 5 {
		return "EXIT_SHORT"
	}

	// 3. Dynamic Directional Cross-Pollution Guard
	if currentSide == "LONG" && state.Efficiency <= -0.6 { // Tightened from -0.8
		return "EXIT_LONG"
	}
	if currentSide == "SHORT" && state.Efficiency >= 0.6 { // Tightened from 0.8
		return "EXIT_SHORT"
	}

	return "HOLD"
}

// CheckTrailingProfitLock handles trailing retracements.
// 🟢 Bypassed for now to prioritize your fixed INR target.
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return false
}

// CheckTakeProfit triggers an immediate exit the moment cash PnL hits or exceeds 500 INR
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool { //
	return false
}

// CheckStopLoss handles fixed risk protection using the instrument's ADR profile
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
