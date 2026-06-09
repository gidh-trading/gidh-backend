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

func (r *TimeBasedRouter) selectStrategy(state *InstrumentState) Strategy {
	// Defaults directly to the morning strategy card
	return r.morningStrategy
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	if state.MinutesSinceOpen > 10 {
		return "HOLD"
	}
	return r.selectStrategy(state).CheckEntry(state)
}

func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	return r.selectStrategy(state).CheckExit(state, currentSide)
}

func (r *TimeBasedRouter) CheckTrailingProfitLock(state *InstrumentState, currentSide string) bool {
	return r.selectStrategy(state).CheckTrailingProfitLock(state, currentSide)
}

func (r *TimeBasedRouter) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.selectStrategy(state).CheckTakeProfit(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.selectStrategy(state).CheckStopLoss(state, currentSide, averagePrice, netQty)
}
