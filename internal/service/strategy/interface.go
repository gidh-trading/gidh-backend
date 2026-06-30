package strategy

import "gidh-backend/internal/service/models"

// Config encapsulates strategy-specific runtime thresholds and risk guardrails.
type Config struct {
	StartTradingTime   int
	EndTradingTime     int
	ForceExitTime      int
	HardStopLossINR    float64
	TakeProfitINR      float64
	MaximumTradesCount int

	// 🟢 New Trailing Stop Loss Parameters
	TrailActivationINR float64
	TrailCallbackINR   float64
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
