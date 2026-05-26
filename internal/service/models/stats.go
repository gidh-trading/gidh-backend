package models

import (
	"bytes"
	"fmt"
	"time"
)

// =====================================================================
// 1. STATEFUL ENGINE TRACKING MODEL (MEMORY LAYER)
// =====================================================================

// VolumeRegimeSession tracks continuous participation expansions in memory
// completely independent of standard time-series candle boundaries.
type VolumeRegimeSession struct {
	Active           bool      `json:"active"`             // true if volume is currently expanded above thresholds
	Token            uint32    `json:"instrument_token"`   // Unique asset identifier
	StockName        string    `json:"stock_name"`         // Asset ticker name
	StartPrice       float64   `json:"start_price"`        // Price locked in at the exact birth moment of the burst
	CurrentPrice     float64   `json:"current_price"`      // Last known price updated tick-by-tick
	StartTime        time.Time `json:"start_time"`         // Timestamp marking when the burst crossed limits
	StartMinuteIndex int       `json:"start_minute_index"` // Locks in original baseline entry bucket index
	PeakVolumeRank   int       `json:"peak_volume_rank"`   // Highest recorded volume coordinate reached during life
}

// =====================================================================
// 2. STATELESS EVENT TELEMETRY MODEL (BROADCAST & DB VIEW LAYER)
// =====================================================================

// VolumeRegimeSnapshot represents an immutable view of a regime session
// frozen at a specific moment in time. It is used for throttled 1-second WebSocket
// broadcasts and final database persistence.
type VolumeRegimeSnapshot struct {
	Timestamp       time.Time `json:"timestamp"` // Master partition time for TimescaleDB
	InstrumentToken uint32    `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	MinuteIndex     int       `json:"minute_index"`

	Active      bool            `json:"active"`       // true = session is ongoing, false = concluded/terminated
	AnomalyType AnomalyType     `json:"anomaly_type"` // Type-safe classification string: "VOLUME_BURST" or "ABSORPTION"
	Direction   RegimeDirection `json:"direction"`    // Type-safe directional bias: 1 = Bullish, -1 = Bearish, 0 = Flat

	// --- Lifespan Boundaries ---
	StartTime time.Time `json:"start_time"` // Exact tick timestamp when participation crossed >= P90
	EndTime   time.Time `json:"end_time"`   // Exact tick timestamp when participation collapsed below threshold

	// --- Core Execution Metrics ---
	StartPrice   float64 `json:"start_price"`
	CurrentPrice float64 `json:"current_price"`
	SignedMove   float64 `json:"signed_move"`
	AbsMove      float64 `json:"abs_move"`

	// --- Multi-Minute Percentile Ranks ---
	PeakVolumeRank   int `json:"peak_volume_rank"`
	CurrentPriceRank int `json:"current_price_rank"`
}

// =====================================================================
// VOLUME REGIME ANOMALY ENUMS
// =====================================================================

// AnomalyType defines the first-class categories for continuous participation windows.
type AnomalyType int

const (
	AnomalyNone        AnomalyType = iota // No active participation expansion
	AnomalyVolumeBurst                    // Sustained aggressive volume with confirmed displacement
	AnomalyAbsorption                     // Sustained aggressive volume meeting passive liquidity walls
)

// String representation for internal logging, diagnostics, and DB string mapping.
func (a AnomalyType) String() string {
	switch a {
	case AnomalyVolumeBurst:
		return "BURST"
	case AnomalyAbsorption:
		return "ABSORPTION"
	default:
		return "NONE"
	}
}

// MarshalJSON converts the internal integer enum to a clean readable string for UI streaming.
func (a AnomalyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", a.String())), nil
}

// UnmarshalJSON reads string signatures back into type-safe Go enum integers.
func (a *AnomalyType) UnmarshalJSON(b []byte) error {
	str := string(bytes.Trim(b, `"`))
	switch str {
	case "BURST":
		*a = AnomalyVolumeBurst
	case "ABSORPTION":
		*a = AnomalyAbsorption
	default:
		*a = AnomalyNone
	}
	return nil
}

// =====================================================================
// DIRECTIONAL CANVAS ENUMS
// =====================================================================

// RegimeDirection preserves the true directional skew of a continuous window.
type RegimeDirection int

const (
	DirectionFlat    RegimeDirection = 0  // No structural displacement or complete equilibrium
	DirectionBullish RegimeDirection = 1  // Upward expansion (Aggressive Buying / Passive Sell Absorption)
	DirectionBearish RegimeDirection = -1 // Downward expansion (Aggressive Selling / Passive Buy Absorption)
)

// String representation for direct directional debugging or front-end classification layers.
func (d RegimeDirection) String() string {
	switch d {
	case DirectionBullish:
		return "BULLISH"
	case DirectionBearish:
		return "BEARISH"
	default:
		return "FLAT"
	}
}
