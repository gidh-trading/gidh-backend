package strategy

import "gidh-backend/internal/service/models"

// Strategy isolates execution decisions into clean, distinct logical tracks.
type Strategy interface {
	Name() string

	// CheckEntry evaluates setups when the position is FLAT
	CheckEntry(state *InstrumentState, bar *models.Bar) string // Returns "GO_LONG", "GO_SHORT", or "HOLD"

	// CheckExit evaluates indicator trend breakdowns when in an ACTIVE trade
	CheckExit(state *InstrumentState, currentSide string) string // Returns "EXIT_LONG", "EXIT_SHORT", or "HOLD"

	// CheckTakeProfit evaluates if your profit targets have been met
	CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool

	// CheckStopLoss evaluates if your safety risk limits have been breached
	CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int) bool

	OnEntryCommit(state *InstrumentState, symbol string)
}
