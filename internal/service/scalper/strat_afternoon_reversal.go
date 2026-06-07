package scalper

type AfternoonReversalStrategy struct{}

func NewAfternoonReversalStrategy() *AfternoonReversalStrategy { return &AfternoonReversalStrategy{} }

func (s *AfternoonReversalStrategy) Name() string { return "Afternoon_Mean_Reversion" }

func (s *AfternoonReversalStrategy) CheckEntry(state *InstrumentState) string {
	return "HOLD"
}

func (s *AfternoonReversalStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}
