package strategy

import "gidh-backend/internal/service/models"

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
	Name() string
	Config() *Config
	CheckEntry(state *InstrumentState) string
	CheckExit(state *InstrumentState, currentSide string) string
	OnEntryCommit(state *InstrumentState, symbol string)

	CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int, profiles map[string]*models.InstrumentProfile) bool
	CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int, percentiles map[string]*models.VWAPDistancePercentile) bool
}
