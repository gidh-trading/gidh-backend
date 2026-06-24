package strategy

type TimeBasedRouter struct {
	adrReversionStrategy Strategy
}

func NewTimeBasedRouter(adrReversionStrat Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		adrReversionStrategy: adrReversionStrat,
	}
}

func (r *TimeBasedRouter) Name() string { return "Institutional_Ledger_PassThrough_Router" }

// selectStrategy simplifies to always returning your 1 primary execution strategy
func (r *TimeBasedRouter) selectStrategy(state *InstrumentState) Strategy {
	return r.adrReversionStrategy
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	// Only allow evaluation if the stock is currently completely flat
	if state.CurrentSetupPhase == PhaseActiveTrade {
		return "HOLD"
	}
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
	activeStrat := r.selectStrategy(state)

	// Bind strategy identity directly onto the shared instrument state block
	state.ActiveStrategyName = activeStrat.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]bool)
	}
	state.StrategyHistory[activeStrat.Name()] = true
}
