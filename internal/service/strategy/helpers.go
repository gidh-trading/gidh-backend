package strategy

// evaluateVWAPBand checks if the current price has cleared a volatility-adjusted band around the VWAP.
// It directly extracts the real-time execution variables from the InstrumentState object.
// multiplier defines how much of the ATR constitutes the band's channel width (e.g., 0.05 = 5% of daily ATR).
func evaluateVWAPBand(state *InstrumentState, multiplier float64) string {
	if state == nil || state.LiveSessionVWAP <= 0.0 {
		return "INSIDE_BAND"
	}

	currentPrice := state.LatestPrice
	vwap := state.LiveSessionVWAP

	// Fallback to a tight nominal 0.1% buffer if ATR is unpopulated or invalid
	var cushion float64 = vwap * 0.001
	if state.Profile.ATR14 > 0.0 {
		cushion = state.Profile.ATR14 * multiplier
	}

	bandUpper := vwap + cushion
	bandLower := vwap - cushion

	switch {
	case currentPrice > bandUpper:
		return "ABOVE_BAND"
	case currentPrice < bandLower:
		return "BELOW_BAND"
	default:
		return "INSIDE_BAND"
	}
}
