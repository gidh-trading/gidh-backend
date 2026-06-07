package agent

// CheckGlobalEmergencyBrackets acts as a universal disaster shield.
func (sa *ScalperAgent) CheckGlobalEmergencyBrackets(state *InstrumentState, entryPrice float64, currentSide string) bool {
	if entryPrice <= 0 {
		return false
	}

	// Hard max trailing loss limit parameters (e.g., 1.5% maximum degradation)
	const maxEmergencyRiskPct = 1.5

	if currentSide == "LONG" {
		lossPct := ((entryPrice - state.LatestPrice) / entryPrice) * 100
		if lossPct >= maxEmergencyRiskPct {
			return true
		}
	}

	if currentSide == "SHORT" {
		lossPct := ((state.LatestPrice - entryPrice) / entryPrice) * 100
		if lossPct >= maxEmergencyRiskPct {
			return true
		}
	}

	return false
}
