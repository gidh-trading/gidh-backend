package scalper

import "time"

type TimeBasedRouter struct {
	MorningStrat   Strategy
	AfternoonStrat Strategy
}

func NewTimeBasedRouter(morning Strategy, afternoon Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		MorningStrat:   morning,
		AfternoonStrat: afternoon, // <-- FIXED: Added missing assignment
	}
}

func (r *TimeBasedRouter) Name() string {
	return "Dynamic_Time_Router"
}

// CheckEntry looks at the clock first, then hands off the choice to the correct card
func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckEntry(state)
	}

	return r.AfternoonStrat.CheckEntry(state)
}

// CheckExit acts as the same traffic cop for exits
func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckExit(state, currentSide)
	}
	return r.AfternoonStrat.CheckExit(state, currentSide)
}
