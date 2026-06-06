package agent

// GenerateSignal handles entry and dynamic context exit calculations based on your dual queues.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	state, exists := sa.Registry[symbol]
	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// ENTRY ALGORITHMIC MATRIX (When account exposure is FLAT)
	// ------------------------------------------------------------------------
	if currentSide == "FLAT" || currentSide == "" {
		if sa.EvaluateDualQueueEntry(state) {
			return "GO_SHORT"
		}
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// DYNAMIC CONTEXTUAL EXIT MATRIX (When position is active)
	// ------------------------------------------------------------------------
	if currentSide == "SHORT" {
		// Rule A: Immediate structural context invalidation (e.g. tape shifts into absorption)
		if state.LatestDirection == "BULLISH_ABSORPTION" {
			return "EXIT_SHORT"
		}

		// Rule B: Cross-check running micro-queues to identify targets or protective trail violations
		if sa.EvaluateDualQueueBrackets(state, entryPrice) {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// EvaluateDualQueueEntry compares recent velocity trends against time-duration metrics
func (sa *ScalperAgent) EvaluateDualQueueEntry(state *InstrumentState) bool {
	// Microscopic layer verification check (Volume rank from incoming enrichment)
	if state.LatestVolumeRank < 6 || state.LatestDirection != "BEARISH" {
		return false
	}

	// Macro layer verification check: Confirm if price is trading below the 5-minute historical mean
	var timeSum float64 = 0.0
	for _, t := range state.TimeQueue {
		timeSum += t.Price
	}
	
	avgPrice5m := timeSum / float64(len(state.TimeQueue))

	if state.LatestPrice >= avgPrice5m {
		return false // Price resides above short-term duration average; filter entry
	}

	return true
}
