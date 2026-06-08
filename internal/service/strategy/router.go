package strategy

import "time"

type TimeBasedRouter struct {
	MorningStrat   Strategy
	AfternoonStrat Strategy
	loc            *time.Location // Cached timezone location pointer
}

func NewTimeBasedRouter(morning Strategy, afternoon Strategy) *TimeBasedRouter {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return &TimeBasedRouter{
		MorningStrat:   morning,
		AfternoonStrat: afternoon,
		loc:            loc,
	}
}

func (r *TimeBasedRouter) Name() string {
	return "Dynamic_Time_Router"
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	if r.isMorning(state.LastUpdated) {
		return r.MorningStrat.CheckEntry(state)
	}
	return r.AfternoonStrat.CheckEntry(state)
}

func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	if r.isMorning(state.LastUpdated) {
		return r.MorningStrat.CheckExit(state, currentSide)
	}
	return r.AfternoonStrat.CheckExit(state, currentSide)
}

func (r *TimeBasedRouter) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if r.isMorning(state.LastUpdated) {
		return r.MorningStrat.CheckTakeProfit(state, currentSide, averagePrice, netQty)
	}
	return r.AfternoonStrat.CheckTakeProfit(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	if r.isMorning(state.LastUpdated) {
		return r.MorningStrat.CheckStopLoss(state, currentSide, averagePrice, netQty)
	}
	return r.AfternoonStrat.CheckStopLoss(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) isMorning(t time.Time) bool {
	localTime := t.In(r.loc)
	hour := localTime.Hour()
	minute := localTime.Minute()

	// 10:30 AM is equivalent to (hour == 10 && minute >= 30) or any hour >= 11
	if hour < 10 {
		return true
	}
	if hour == 10 && minute < 30 {
		return true
	}
	return false
}
