package strategy

type TimeBasedRouter struct {
	combinedMoodStrategy Strategy
}

// NewTimeBasedRouter instantiates the system router wrapped around a single strategy core.
func NewTimeBasedRouter(combinedMoodStrat Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		combinedMoodStrategy: combinedMoodStrat,
	}
}

func (r *TimeBasedRouter) Name() string { return "Institutional_Ledger_PassThrough_Router" }

func (r *TimeBasedRouter) selectStrategy(state *InstrumentState) Strategy {
	// Directly pass through to our single institutional ledger strategy card
	return r.combinedMoodStrategy
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	// Pass directly to the underlying ledger strategy card which manages its own
	// chronological locks, opening ranges, and signed decay metrics safely.
	return r.selectStrategy(state).CheckEntry(state)
}

func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	return r.selectStrategy(state).CheckExit(state, currentSide)
}
func (r *TimeBasedRouter) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.selectStrategy(state).CheckTakeProfit(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool {
	return r.selectStrategy(state).CheckStopLoss(state, currentSide, averagePrice, netQty)
}

func (r *TimeBasedRouter) OnEntryCommit(state *InstrumentState, symbol string) {
	r.selectStrategy(state).OnEntryCommit(state, symbol)
}
