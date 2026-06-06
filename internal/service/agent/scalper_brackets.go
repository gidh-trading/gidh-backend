package agent

// EvaluateDualQueueBrackets tracks dynamic protection levels by measuring time and count metrics
func (sa *ScalperAgent) EvaluateDualQueueBrackets(state *InstrumentState, entryPrice float64, currentSide string) bool {
	// 1. DYNAMIC TIME-BASED MONITORING
	// FIX: Use unlocked variant
	timeData5m := sa.getRecentMinutesDataUnlocked(state, 5)

	var totalTimeVolume float64 = 0.0
	var totalTimeValue float64 = 0.0

	for _, tx := range timeData5m {
		totalTimeVolume += tx.Volume
		totalTimeValue += tx.Price * tx.Volume
	}

	if totalTimeVolume > 0 {
		timeVwap5m := totalTimeValue / totalTimeVolume
		// If SHORT, exit if price spikes above the 5m VWAP
		if currentSide == "SHORT" && state.LatestPrice >= timeVwap5m {
			return true
		}
		// If LONG, exit if price breaks below the 5m VWAP
		if currentSide == "LONG" && state.LatestPrice <= timeVwap5m {
			return true
		}
	}

	// 2. DYNAMIC TRANSACTION-BASED MONITORING
	// FIX: Use unlocked variant
	txData50 := sa.getLastTransactionsUnlocked(state, 50)

	var totalTxVolume float64 = 0.0
	var totalTxValue float64 = 0.0

	for _, tx := range txData50 {
		totalTxVolume += tx.Volume
		totalTxValue += tx.Price * tx.Volume
	}

	if totalTxVolume > 0 {
		txVwap50 := totalTxValue / totalTxVolume
		distFromTxVwap := ((state.LatestPrice - txVwap50) / txVwap50) * 100

		// Asymmetric logic protection drop
		if currentSide == "LONG" && distFromTxVwap <= -1.50 {
			return true
		}
		if currentSide == "SHORT" && distFromTxVwap >= 1.50 {
			return true
		}
	}

	return false
}
