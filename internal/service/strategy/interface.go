package strategy

// Config encapsulates strategy-specific runtime thresholds and risk guardrails.
type Config struct {
	StartTradingTime   int     // e.g., 920 (09:20 AM)
	EndTradingTime     int     // e.g., 955 (09:55 AM)
	ForceExitTime      int     // e.g., 1015 (10:15 AM)
	HardStopLossINR    float64 // e.g., -300.0
	TakeProfitINR      float64 // e.g., 600.0
	MaximumTradesCount int     // Maximum allowed trades per stock for this explicit strategy today
}

// Strategy isolates execution decisions into clean, distinct logical tracks.
type Strategy interface {
	// Name returns the unique identity key for the strategy
	Name() string

	// Config returns the configuration parameters mapped to this strategy instance
	Config() *Config

	// CheckEntry evaluates setups when the position is FLAT for this strategy context
	CheckEntry(state *InstrumentState) string // Returns "GO_LONG", "GO_SHORT", or "HOLD"

	// CheckExit evaluates indicator trend breakdowns when in an ACTIVE trade
	CheckExit(state *InstrumentState, currentSide string) string // Returns "EXIT_LONG", "EXIT_SHORT", or "HOLD"

	// CheckTakeProfit evaluates if your profit targets have been met
	CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool

	// CheckStopLoss evaluates if your safety risk limits have been breached
	CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool

	// OnEntryCommit provides a hook to mutate or enrich strategy-isolated state properties immediately upon execution commit
	OnEntryCommit(state *InstrumentState, symbol string)
}
