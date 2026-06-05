package models

type AbsorptionWall struct {
	Direction       string  `json:"direction"`        // "ABSORPTION_SHORT" or "ABSORPTION_LONG"
	AbsorptionPrice float64 `json:"absorption_price"` // Exact execution coordinate of the iceberg wall
}

type HqIntelligencePayload struct {
	Direction   string             `json:"direction"` // "BULLISH", "BEARISH", "NONE", "ABSORPTION_SHORT", "ABSORPTION_LONG"
	FlowMetrics TapeTelemetryUnits `json:"flow_metrics"`
}

type TapeTelemetryUnits struct {
	BiasScore    float64          `json:"bias_score"`
	VwpDelta     float64          `json:"vwp_delta"`
	Efficiency   float64          `json:"efficiency"`
	IsAbsorption bool             `json:"is_absorption"`
	ActiveWalls  []AbsorptionWall `json:"active_walls"` // Fixed canvas bubble storage array
}
