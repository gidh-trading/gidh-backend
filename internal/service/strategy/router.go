package strategy

type TimeBasedRouter struct {
	ledgerStrategy Strategy
}

// NewTimeBasedRouter instantiates the system router wrapped around a single strategy core.
func NewTimeBasedRouter(ledger Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		ledgerStrategy: ledger,
	}
}

func (r *TimeBasedRouter) Name() string { return "Institutional_Ledger_PassThrough_Router" }

func (r *TimeBasedRouter) selectStrategy(state *InstrumentState) Strategy {
	// Directly pass through to our single institutional ledger strategy card
	return r.ledgerStrategy
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	// Old 10-minute time gate restriction is completely removed.
	// The ledger strategy handles its own structural time-at-price validation safely.
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
