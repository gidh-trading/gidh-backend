package agent

// EvaluateDualQueueBrackets tracks dynamic protection levels by measuring time and count metrics
func (sa *ScalperAgent) EvaluateDualQueueBrackets(state *InstrumentState, entryPrice float64) bool {
	// 1. DYNAMIC TIME-BASED MONITORING: Calculate moving VWAP over the rolling 5-minute window
	var totalTimeVolume float64 = 0.0
	var totalTimeValue float64 = 0.0

	for _, tx := range state.TimeQueue {
		totalTimeVolume += tx.Volume
		totalTimeValue += tx.Price * tx.Volume
	}

	if totalTimeVolume <= 0 {
		return false
	}
	timeVwap5m := totalTimeValue / totalTimeVolume

	// Dynamic Stop Loss: Invalidation if price rallies back and crosses the 5-minute VWAP anchor line
	if state.LatestPrice >= timeVwap5m {
		return true // Trigger exit
	}

	// 2. DYNAMIC TRANSACTION-BASED MONITORING: Calculate moving VWAP over the last 50 transactions
	var totalTxVolume float64 = 0.0
	var totalTxValue float64 = 0.0

	for _, tx := range state.TxQueue {
		totalTxVolume += tx.Volume
		totalTxValue += tx.Price * tx.Volume
	}

	if totalTxVolume > 0 {
		txVwap50 := totalTxValue / totalTxVolume

		// Dynamic Take Profit: Price falls significantly below transactional value core (e.g. -1.50%)
		distFromTxVwap := ((state.LatestPrice - txVwap50) / txVwap50) * 100
		if distFromTxVwap <= -1.50 {
			return true // Trigger profit-taking
		}
	}

	return false
}
