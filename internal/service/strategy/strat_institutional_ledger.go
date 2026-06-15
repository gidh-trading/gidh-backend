package strategy

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

// Fixed Entry Mechanics: Strip out trailing averages, volume ranks, and dynamic deltas
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// Priority 1: Prevent duplicate execution on the exact same bar
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	currentEff := state.NetEfficiency

	// --- 🟢 SIMPLIFIED ENTRY LONG TRIGGER ---
	// Rule 1: Efficiency is strictly greater than 35
	// Rule 2: Latest price is trading cleanly above the live VWAP
	if currentEff > 35.0 && state.LatestPrice > state.LiveSessionVWAP {
		return "GO_LONG"
	}

	// --- 🔴 SIMPLIFIED ENTRY SHORT TRIGGER ---
	// Rule 1: Efficiency is strictly less than -35
	// Rule 2: Latest price is trading cleanly below the live VWAP
	if currentEff < -35.0 && state.LatestPrice < state.LiveSessionVWAP {
		return "GO_SHORT"
	}

	return "HOLD"
}

// Keeping baseline exit conditions intact for Risk Manager routing
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	dynamicCushion := s.VwapBufferPct
	if state.Profile != nil && state.Profile.ADRPct > 4.0 {
		dynamicCushion = s.VwapBufferPct * 1.5
	}

	if currentSide == "LONG" {
		if state.LatestPrice < (state.LiveSessionVWAP * (1.0 - dynamicCushion)) {
			return "EXIT_LONG"
		}
		if state.NormalizedVwapDistance > 2.8 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		if state.LatestPrice > (state.LiveSessionVWAP * (1.0 + dynamicCushion)) {
			return "EXIT_SHORT"
		}
		if state.NormalizedVwapDistance < -2.8 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	// Let's keep this clean while we verify entries are processing orders correctly
	return false
}

func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return false
}

func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
