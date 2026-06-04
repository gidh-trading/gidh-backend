package models

type HqIntelligencePayload struct {
	Direction   string             `json:"direction"` // "BULLISH", "BEARISH", "NONE"
	LiveMetrics HqLiveMetricsUnits `json:"live_metrics"`
}

type HqLiveMetricsUnits struct {
	VwpDelta   float64 `json:"vwp_delta"`
	Efficiency float64 `json:"efficiency"`
}
