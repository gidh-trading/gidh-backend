package agent

import "fmt"

// GenerateSignal handles the Morning Momentum Flow (MMF) strategy.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	state, exists := sa.Registry[symbol]
	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// 1. STRICT TIME GATING (9:15 AM to 10:30 AM IST)
	// Assuming state.LastUpdated represents the tick time.
	// You may need to ensure the timezone is set to IST if trading Indian markets.
	hour, min, _ := state.LastUpdated.Clock()
	marketMinutes := hour*60 + min
	fmt.Println(marketMinutes)

	// 9:15 = 555 mins, 10:30 = 630 mins
	if marketMinutes < 555 || marketMinutes > 630 {
		// If we are holding a position past 10:30, force an exit
		if currentSide == "SHORT" {
			return "EXIT_SHORT"
		} else if currentSide == "LONG" {
			return "EXIT_LONG"
		}
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// ENTRY ALGORITHMIC MATRIX (When account exposure is FLAT)
	// ------------------------------------------------------------------------
	if currentSide == "FLAT" || currentSide == "" {
		signal := sa.EvaluateMorningStrategy(state)
		if signal != "" {
			return signal
		}
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// DYNAMIC CONTEXTUAL EXIT MATRIX (When position is active)
	// ------------------------------------------------------------------------
	if currentSide == "SHORT" {
		// Tape shifts against us sharply
		if state.LatestDirection == "BULLISH_ABSORPTION" || state.LatestDirection == "STRONG_BULLISH" {
			return "EXIT_SHORT"
		}
		// Cross-check running micro-queues (from your scalper_brackets.go)
		if sa.EvaluateDualQueueBrackets(state, entryPrice) {
			return "EXIT_SHORT"
		}
	}

	if currentSide == "LONG" {
		// Tape shifts against us sharply
		if state.LatestDirection == "BEARISH_ABSORPTION" || state.LatestDirection == "STRONG_BEARISH" {
			return "EXIT_LONG"
		}
		// Assuming you adapt EvaluateDualQueueBrackets for LONG positions too
		if sa.EvaluateDualQueueBrackets(state, entryPrice) { // You'll need to pass currentSide to this function eventually
			return "EXIT_LONG"
		}
	}

	return "HOLD"
}

// EvaluateMorningStrategy calculates the rolling VWAP and looks for momentum bursts
func (sa *ScalperAgent) EvaluateMorningStrategy(state *InstrumentState) string {
	// We need a decent volume burst to consider an entry.
	// Don't trade in low-activity chop.
	if state.LatestVolumeRank < 6 {
		return ""
	}

	// Calculate the Rolling Session VWAP using the TimeQueue (which holds up to 60 mins of data)
	var totalVolume float64 = 0.0
	var totalValue float64 = 0.0

	for _, t := range state.TimeQueue {
		totalVolume += t.Volume
		totalValue += t.Price * t.Volume
	}

	if totalVolume == 0 {
		return ""
	}

	sessionVWAP := totalValue / totalVolume

	// LOGIC: Momentum Alignment
	// If current price is below the Morning VWAP, we are in a Bearish flow.
	if state.LatestPrice < sessionVWAP {
		// Look for a bearish micro-burst to jump on
		if state.LatestDirection == "BEARISH" || state.LatestDirection == "STRONG_BEARISH" {
			return "GO_SHORT"
		}
	}

	// If current price is above the Morning VWAP, we are in a Bullish flow.
	if state.LatestPrice > sessionVWAP {
		// Look for a bullish micro-burst
		if state.LatestDirection == "BULLISH" || state.LatestDirection == "STRONG_BULLISH" {
			return "GO_LONG"
		}
	}

	return ""
}
