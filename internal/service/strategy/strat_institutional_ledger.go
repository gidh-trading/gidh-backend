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

func (s *InstitutionalLedgerStrategy) CheckEntry(state *InstrumentState) string {
	if !state.LastTradedBarTime.IsZero() && state.LastUpdated.Equal(state.LastTradedBarTime) {
		return "HOLD"
	}

	historyLength := len(state.NetEfficiencyHistory)
	if historyLength < 4 {
		return "HOLD"
	}

	currentEff := state.NetEfficiency
	previousEff := state.NetEfficiencyHistory[historyLength-2]

	// 3-Bar historical evaluation window (Fixes Bug #2)
	trailing3AvgEff := (state.NetEfficiencyHistory[historyLength-4] +
		state.NetEfficiencyHistory[historyLength-3] +
		previousEff) / 3.0

	// --- 🟢 VERSION 1 HIGH-CONVICTION LONG TRIGGER ---
	if state.LatestVolumeRank >= 6 { // 1. Volume >= P90
		if currentEff > 50.0 { // 2. Efficiency > +50
			if trailing3AvgEff > 0.0 { // 3. Trailing 3-bar average efficiency > 0

				// 🛠️ FIX Bug #3: Expansion verification threshold applied (effDelta > 15)
				effDelta := currentEff - previousEff
				if effDelta > 15.0 {

					// 🚀 Session Acceptance Layer (Filter #7)
					if state.NormalizedVwapDistance > 0 && state.NormalizedVwapDistance < 1.5 {
						if state.TimePctAboveVwap > 0.35 {
							return "GO_LONG"
						}
					}
				}
			}
		}
	}

	// --- 🔴 VERSION 1 HIGH-CONVICTION SHORT TRIGGER ---
	if state.LatestVolumeRank >= 6 { // 1. Volume >= P90
		if currentEff < -50.0 { // 2. Efficiency < -50
			if trailing3AvgEff < 0.0 { // 3. Trailing 3-bar average efficiency < 0

				// 🛠️ FIX Bug #3: Expansion breakdown verification threshold applied (effDelta < -15)
				effDelta := currentEff - previousEff
				if effDelta < -15.0 {

					if state.NormalizedVwapDistance < 0 && state.NormalizedVwapDistance > -1.5 {
						if state.TimePctAboveVwap < 0.25 {
							return "GO_SHORT"
						}
					}
				}
			}
		}
	}

	return "HOLD"
}

func (s *InstitutionalLedgerStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	dynamicCushion := s.VwapBufferPct
	if state.Profile != nil && state.Profile.ADRPct > 4.0 {
		dynamicCushion = s.VwapBufferPct * 1.5
	}

	currentEff := state.NetEfficiency

	if currentSide == "LONG" {
		// Exit Pillar 1: Pure Trend Failure Invalidation (Fixes Bug #4 & #5 - No more Zero cross overlap conflict)
		if state.LatestPrice < (state.LiveSessionVWAP * (1.0 - dynamicCushion)) {
			return "EXIT_LONG" // "VWAP Failure"
		}

		// Exit Pillar 2: 🛡️ Peak Efficiency Half-Life Decay Trailing Mechanism (Fixes Bug #5 & #6)
		// Read-only logic: Engine is the exclusive manager of state.PeakEfficiency mutations
		if state.PeakEfficiency > 50.0 {
			decayThreshold := state.PeakEfficiency * 0.50
			if currentEff < decayThreshold {
				return "EXIT_LONG" // "Peak Efficiency Decay"
			}
		}

		if state.NormalizedVwapDistance > 2.8 {
			return "EXIT_LONG" // "Volatility Extension Climax"
		}
	}

	if currentSide == "SHORT" {
		// Exit Pillar 1: Pure Trend Failure Invalidation
		if state.LatestPrice > (state.LiveSessionVWAP * (1.0 + dynamicCushion)) {
			return "EXIT_SHORT" // "VWAP Failure"
		}

		// Exit Pillar 2: 🛡️ Peak Efficiency Half-Life Decay Trailing Mechanism
		if state.PeakEfficiency > 50.0 {
			decayThreshold := state.PeakEfficiency * 0.50
			if currentEff > -decayThreshold {
				return "EXIT_SHORT" // "Peak Efficiency Decay"
			}
		}

		if state.NormalizedVwapDistance < -2.8 {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

func (s *InstitutionalLedgerStrategy) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
func (s *InstitutionalLedgerStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return false
}
