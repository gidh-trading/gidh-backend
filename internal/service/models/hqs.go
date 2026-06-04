package models

// HqIntelligencePayload is the standardized JSON object nested directly
// inside your gidh_bars database table and WebSocket payloads.
type HqIntelligencePayload struct {
	// Direction labels the core structural state: "BULLISH", "BEARISH", or "NONE"
	Direction string `json:"direction"`

	// FlowMetrics acts as a flexible data vault for current continuous tape vectors.
	FlowMetrics TapeTelemetryUnits `json:"flow_metrics"`
}

// TapeTelemetryUnits houses clean, un-spoofable mathematical variables.
type TapeTelemetryUnits struct {
	// BiasScore normalizes immediate order-flow consensus between -1.0 and +1.0.
	// -1.0 = 100% Aggressive Selling Volume (Slamming Bids)
	// +1.0 = 100% Aggressive Buying Volume (Lifting Offers)
	BiasScore float64 `json:"bias_score"`

	// VwpDelta tracks the absolute raw Volume-Weighted Price progress over the window.
	VwpDelta float64 `json:"vwp_delta"`

	// Efficiency evaluates spatial price progress achieved per unit of volume spent.
	Efficiency float64 `json:"efficiency"`
}
