package strategy

type InstitutionalLedgerStrategy struct {
	VwapBufferPct float64
}

func NewInstitutionalLedgerStrategy() *InstitutionalLedgerStrategy {
	return &InstitutionalLedgerStrategy{
		VwapBufferPct: 0.0012, // Base threshold for value optimization bounds
	}
}

func (s *InstitutionalLedgerStrategy) Name() string {
	return "Institutional_Ledger_Alpha_Tuned"
}

// CheckEntry evaluates pure institutional acceleration with strict value parameters.
func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	// 1. 🔒 CHRONOLOGICAL LOCK GATE
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	// --- 🟢 STRUCTURAL LONG ENTRY TRIGGER ---
	if state.ConsecutiveClosesAboveVwap >= 3 {
		// Verify raw efficiency bounds
		if state.NetEfficiency > 8 && state.NetEfficiency < 30 {
			// Confirm positive directional momentum acceleration
			if state.NetEfficiencySlope > 0.5 {
				// Value entry confirmation: price is safely close to the VWAP anchor
				if state.NormalizedVwapDistance > 0 && state.NormalizedVwapDistance < 1.5 {
					if state.TimePctAboveVwap > 0.35 && state.TimePctAboveVwap < 0.75 {
						return "GO_LONG"
					}
				}
			}
		}
	}

	// --- 🔴 STRUCTURAL SHORT ENTRY TRIGGER ---
	if state.ConsecutiveClosesBelowVwap >= 3 {
		// Verify raw efficiency bounds
		if state.NetEfficiency < -8 && state.NetEfficiency > -30 {
			// Confirm negative directional momentum acceleration
			if state.NetEfficiencySlope < -0.5 {
				// Value entry confirmation: price is near breakdown value anchors
				if state.NormalizedVwapDistance < 0 && state.NormalizedVwapDistance > -1.5 {
					if state.TimePctAboveVwap < 0.25 {
						return "GO_SHORT"
					}
				}
			}
		}
	}

	return "HOLD"
}

// CheckExit implements a microstructural Profit & Invalidation Lock based on ADRPct volatility
func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	// Dynamically scale cushions based on ADRPct metadata to prevent micro-stops on high-velocity setups
	dynamicCushion := s.VwapBufferPct
	if state.Profile != nil && state.Profile.ADRPct > 4.0 {
		dynamicCushion = s.VwapBufferPct * 1.5 // Expand buffer slightly for higher ADR names
	}

	if currentSide == "LONG" {
		// 1. Core Price-Action Invalidation (VWAP Violation Cushion)
		if state.LatestPrice < (state.LiveSessionVWAP * (1.0 - dynamicCushion)) {
			return "EXIT_LONG"
		}
		// 2. Momentum Evaporation Filter (Absolute Reversal OR Aggressive Negative Deceleration)
		if state.NetEfficiency <= -2 || state.NetEfficiencySlope < -2.0 {
			return "EXIT_LONG"
		}
		// 3. Volatility Extension Climax Cap (Overextended Z-Score anchor target reached)
		if state.NormalizedVwapDistance > 2.8 {
			return "EXIT_LONG"
		}
	}

	if currentSide == "SHORT" {
		// 1. Core Price-Action Invalidation (VWAP Violation Cushion)
		if state.LatestPrice > (state.LiveSessionVWAP * (1.0 + dynamicCushion)) {
			return "EXIT_SHORT"
		}
		// 2. Momentum Evaporation Filter (Absolute Reversal OR Aggressive Positive Re-acceleration)
		if state.NetEfficiency >= 2 || state.NetEfficiencySlope > 2.0 {
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
	// If a position has printed significant structural extension, lock down gains on exhaustion
	if currentSide == "LONG" && state.NormalizedVwapDistance > 1.8 && state.NetEfficiencySlope < -0.1 {
		return true
	}
	if currentSide == "SHORT" && state.NormalizedVwapDistance < -1.8 && state.NetEfficiencySlope > 0.1 {
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
