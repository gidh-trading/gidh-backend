package scalper

// Strategy defines the exact blueprint for any isolated rule set you create.
type Strategy interface {
	Name() string
	CheckEntry(state *InstrumentState) string                    // Returns "GO_LONG", "GO_SHORT", or "HOLD"
	CheckExit(state *InstrumentState, currentSide string) string // Returns "EXIT_LONG", "EXIT_SHORT", or "HOLD"
}
