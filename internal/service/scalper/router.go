package scalper

import "time"

type TimeBasedRouter struct {
	MorningStrat   Strategy
	AfternoonStrat Strategy
}

func NewTimeBasedRouter(morning Strategy, afternoon Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		MorningStrat:   morning,
		AfternoonStrat: afternoon,
	}
}

func (r *TimeBasedRouter) Name() string {
	return "Dynamic_Time_Router"
}

// 1. CheckEntry looks at the clock first, then hands off the choice to the correct card
func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckEntry(state)
	}

	return r.AfternoonStrat.CheckEntry(state)
}

// 2. CheckExit acts as the traffic cop for technical trend exits
func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckExit(state, currentSide)
	}
	return r.AfternoonStrat.CheckExit(state, currentSide)
}

// 3. CheckTakeProfit routes the cash target verification down to the active card
func (r *TimeBasedRouter) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckTakeProfit(state, currentSide, averagePrice, netQty)
	}
	return r.AfternoonStrat.CheckTakeProfit(state, currentSide, averagePrice, netQty)
}

// 4. CheckStopLoss routes the safety barrier validation down to the active card
func (r *TimeBasedRouter) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	currentHour := state.LastUpdated.In(loc).Hour()

	if currentHour < 12 {
		return r.MorningStrat.CheckStopLoss(state, currentSide, averagePrice, netQty)
	}
	return r.AfternoonStrat.CheckStopLoss(state, currentSide, averagePrice, netQty)
}
