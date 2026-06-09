package strategy

type TimeBasedRouter struct {
	morningStrategy Strategy
}

func NewTimeBasedRouter(morning Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		morningStrategy: morning,
	}
}

func (r *TimeBasedRouter) Name() string { return "Morning_Only_Time_Router" }

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	// Strict Wall: No fresh trade entries are allowed anywhere in the system after 9:25 AM IST
	if state.MinutesSinceOpen > 10 {
		return "HOLD"
	}
	return r.morningStrategy.CheckEntry(state)
}

func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	// Existing active trades can still check their trend-flip exits normally until closed out
	return r.morningStrategy.CheckExit(state, currentSide)
}

func (r *TimeBasedRouter) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.morningStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.morningStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty)
}
