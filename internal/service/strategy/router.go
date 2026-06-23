package strategy

type TimeBasedRouter struct {
	momentumRunStrategy   Strategy
	vwapReversionStrategy Strategy
}

func NewTimeBasedRouter(combinedMoodStrat Strategy, vwapReversionStrat Strategy) *TimeBasedRouter {
	return &TimeBasedRouter{
		momentumRunStrategy:   combinedMoodStrat,
		vwapReversionStrategy: vwapReversionStrat,
	}
}

func (r *TimeBasedRouter) Name() string { return "Institutional_Ledger_PassThrough_Router" }

// selectStrategy handles traffic routing cleanly based on active execution context ownership
func (r *TimeBasedRouter) selectStrategy(state *InstrumentState) Strategy {
	// 🛡️ CRITICAL FIX: If an asset is actively trading, route strictly to the strategy that opened it!
	if state.CurrentSetupPhase == PhaseActiveTrade && state.ActiveStrategyName != "" {
		if state.ActiveStrategyName == r.momentumRunStrategy.Name() {
			return r.momentumRunStrategy
		}
		if state.ActiveStrategyName == r.vwapReversionStrategy.Name() {
			return r.vwapReversionStrategy
		}
	}

	// For entries and flat scripts, route dynamically based on the current timeframe window
	tf := "1m"
	history, ok := state.BarHistory[tf]
	if !ok || len(history) < 1 {
		return r.momentumRunStrategy
	}

	t := history[len(history)-1].Timestamp
	currentTimeInt := t.Hour()*100 + t.Minute()

	if currentTimeInt >= MomentumStartTradingTime && currentTimeInt < ReversionStartTradingTime {
		return r.momentumRunStrategy
	}

	return r.vwapReversionStrategy
}

func (r *TimeBasedRouter) CheckEntry(state *InstrumentState) string {
	// Only allow evaluation if the stock is currently completely flat
	if state.CurrentSetupPhase == PhaseActiveTrade {
		return "HOLD"
	}
	return r.selectStrategy(state).CheckEntry(state)
}

func (r *TimeBasedRouter) CheckExit(state *InstrumentState, currentSide string) string {
	// Evaluates exits using the strategy that owns the active trade
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
