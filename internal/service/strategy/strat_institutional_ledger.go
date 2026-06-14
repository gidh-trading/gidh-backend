package strategy

type InstitutionalLedgerStrategy struct {
	VwapBufferPct float64
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct: 0.0012, // Lowered base threshold for tight value optimization
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned"
}

// CheckEntry evaluates pure institutional footprints with strict anti-chasing locks.
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// 1. 🔒 CHRONOLOGICAL LOCK GATE
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// 2. 🛡️ ANTI-CHOP DEFENSE GATE
	if state.VwapCrossCount > 3 { // Tightened from 4 to cut down noise trading
		return "HOLD"
	}

	netEfficiency := state.Ledger.BullEfficient - state.Ledger.BearEfficient

	// --- 🟢 STRUCTURAL LONG ENTRY TRIGGER ---
	if state.ConsecutiveClosesAboveVwap >= 3 {
		if netEfficiency > 8 && netEfficiency < 30 { // Bounded tighter to avoid chasing peaks

			// Only buy if we are close to the VWAP anchor (Value Entry)
			if state.NormalizedVwapDistance > 0 && state.NormalizedVwapDistance < 1.5 {
				if state.TimePctAboveVwap > 0.35 && state.TimePctAboveVwap < 0.75 {
					return "GO_LONG"
				}
			}
		}
	}

	// --- 🔴 STRUCTURAL SHORT ENTRY TRIGGER ---
	if state.ConsecutiveClosesBelowVwap >= 3 {
		if netEfficiency < -8 && netEfficiency > -30 {

			// Only short if we are near the breakdown value anchor
			if state.NormalizedVwapDistance < 0 && state.NormalizedVwapDistance > -1.5 {
				if state.TimePctAboveVwap < 0.25 {
					return "GO_SHORT"
				}
			}
		}
	}

	return "HOLD"
}

// CheckExit protects performance by implementing an immediate Microstructural Profit Lock
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	netEfficiency := state.Ledger.BullEfficient - state.Ledger.BearEfficient

	// Dynamically adjust the cushion based on the stock's price rank to avoid fast stop-outs
	dynamicCushion := s.VwapBufferPct
	if state.LatestPriceRank > 5 {
		dynamicCushion = s.VwapBufferPct * 1.5 // Expand buffer slightly for higher-velocity names
	}

	if currentSide == "LONG" {
		// 1. Core Price-Action Invalidation
		if state.LatestPrice < (state.LiveSessionVWAP * (1.0 - dynamicCushion)) {
			return "EXIT_LONG"
		}
		// 2. Momentum Evaporation Filter
		if netEfficiency <= -2 {
			return "EXIT_LONG"
		}
		// 3. Volatility Extension Climax Cap
		if state.NormalizedVwapDistance > 2.8 { // Lowered from 3.5 to lock gains earlier
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		// 1. Core Price-Action Invalidation
		if state.LatestPrice > (state.LiveSessionVWAP * (1.0 + dynamicCushion)) {
			return "EXIT_SHORT"
		}
		// 2. Momentum Evaporation Filter
		if netEfficiency >= 2 {
			return "EXIT_SHORT"
		}
		// 3. Volatility Extension Climax Cap
		if state.NormalizedVwapDistance < -2.8 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// CheckTrailingProfitLock forces an exit if the strategy starts giving back open profits
func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	// If a position has printed significant structural extension, lock it down
	if currentSide == "LONG" && state.NormalizedVwapDistance > 1.8 && state.Efficiency < 0.1 {
		return true // Force execution trailing lock
	}
	if currentSide == "SHORT" && state.NormalizedVwapDistance < -1.8 && state.Efficiency > -0.1 {
		return true
	}
	return false
}

func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
