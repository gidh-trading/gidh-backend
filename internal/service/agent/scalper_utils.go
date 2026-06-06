package agent

import "time"

func (sa *ScalperAgent) getMarketMinutes(t time.Time) (int, time.Time) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err == nil {
		t = t.In(loc)
	}
	hour, minute, _ := t.Clock()
	return (hour * 60) + minute, t
}

func (sa *ScalperAgent) handleSessionCloseExits(currentSide string) string {
	if currentSide == "SHORT" {
		return "EXIT_SHORT"
	}
	if currentSide == "LONG" {
		return "EXIT_LONG"
	}
	return "HOLD"
}

func (sa *ScalperAgent) isEngineInCooldown(state *InstrumentState, currentTickTime time.Time) bool {
	return !state.LastExitTime.IsZero() && currentTickTime.Sub(state.LastExitTime) < 5*time.Minute
}

func (sa *ScalperAgent) UpdateOpeningRangeBoundaries(state *InstrumentState, marketMins int) {
	if marketMins >= 555 && marketMins < 560 {
		if !state.OpeningRangeSet {
			state.OpeningHigh = state.LatestPrice
			state.OpeningLow = state.LatestPrice
			state.OpeningRangeSet = true
		} else {
			if state.LatestPrice > state.OpeningHigh {
				state.OpeningHigh = state.LatestPrice
			}
			if state.LatestPrice < state.OpeningLow {
				state.OpeningLow = state.LatestPrice
			}
		}
	}
}

// RegisterPositionClosure updates state memory frames to activate the 5-minute cooldown mechanism
func (sa *ScalperAgent) RegisterPositionClosure(symbol string, completionTime time.Time) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if state, exists := sa.Registry[symbol]; exists {
		state.LastExitTime = completionTime
	}
}
