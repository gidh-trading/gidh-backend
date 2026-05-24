package models

import (
	"bytes"
	"fmt"
)

type TrendMetrics struct {
	PriceTrendDirection int     `json:"price_trend_direction"` // -1 = Falling, 0 = Flat, 1 = Rising
	TenMinuteNetReturn  float64 `json:"ten_minute_net_return"` // Absolute point change
	VelocityPerMinute   float64 `json:"velocity_per_minute"`   // Average change speed
}

// AnomalyType defines our type-safe category enum matrix
type AnomalyType int

const (
	AnomalyNone AnomalyType = iota
	AnomalyVolumeBurst
	AnomalyAbsorption
	AnomalyVolatilitySurge // Example of easily adding a new type
)

// String representation for internal logging or debugging
func (a AnomalyType) String() string {
	switch a {
	case AnomalyVolumeBurst:
		return "VOLUME_BURST"
	case AnomalyAbsorption:
		return "ABSORPTION"
	case AnomalyVolatilitySurge:
		return "VOLATILITY_SURGE"
	default:
		return "NONE"
	}
}

// MarshalJSON converts our internal integer enum to a clean readable string for your UI/DB
func (a AnomalyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", a.String())), nil
}

// UnmarshalJSON allows reading string entries back into type-safe Go enum integers
func (a *AnomalyType) UnmarshalJSON(b []byte) error {
	str := string(bytes.Trim(b, `"`))
	switch str {
	case "VOLUME_BURST":
		*a = AnomalyVolumeBurst
	case "ABSORPTION":
		*a = AnomalyAbsorption
	case "VOLATILITY_SURGE":
		*a = AnomalyVolatilitySurge
	default:
		*a = AnomalyNone
	}
	return nil
}
