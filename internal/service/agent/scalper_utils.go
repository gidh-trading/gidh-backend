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

// RegisterPositionClosure updates state memory to activate the 5-minute cooldown mechanism
func (sa *ScalperAgent) RegisterPositionClosure(symbol string, completionTime time.Time) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if state, exists := sa.Registry[symbol]; exists {
		state.LastExitTime = completionTime
	}
}
