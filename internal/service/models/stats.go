package models

import (
	"bytes"
	"fmt"
	"time"
)

// =====================================================================
// 1. STATEFUL ENGINE TRACKING MODEL (MEMORY LAYER) - UPGRADED
// =====================================================================

// VolumeRegimeSession tracks continuous participation expansions in memory
// completely independent of standard time-series candle boundaries.
type VolumeRegimeSession struct {
	Active           bool            `json:"active"`
	Token            uint32          `json:"instrument_token"`
	StockName        string          `json:"stock_name"`
	StartPrice       float64         `json:"start_price"`
	CurrentPrice     float64         `json:"current_price"`
	SessionHigh      float64         `json:"session_high"` // 👈 Add structural ceiling bounds
	SessionLow       float64         `json:"session_low"`  // 👈 Add structural floor bounds
	StartTime        time.Time       `json:"start_time"`
	StartMinuteIndex int             `json:"start_minute_index"`
	PeakVolumeRank   int             `json:"peak_volume_rank"`
	PeakTickRank     int             `json:"peak_tick_rank"`
	PeakPriceRank    int             `json:"peak_price_rank"`
	Direction        RegimeDirection `json:"direction"`
}

// =====================================================================
// 2. STATELESS EVENT TELEMETRY MODEL (BROADCAST & DB VIEW LAYER)
// =====================================================================

// VolumeRegimeSnapshot represents an immutable statistical footprint written directly to TimescaleDB.
type VolumeRegimeSnapshot struct {
	Timestamp        time.Time       `json:"timestamp"` // Mandatory partitioning timestamp
	InstrumentToken  int32           `json:"instrument_token"`
	StockName        string          `json:"stock_name"`
	MinuteIndex      int             `json:"minute_index"`
	Active           bool            `json:"active"`
	AnomalyType      AnomalyType     `json:"anomaly_type"`
	Direction        RegimeDirection `json:"direction"`
	StartTime        time.Time       `json:"start_time"`
	EndTime          time.Time       `json:"end_time"`
	StartPrice       float64         `json:"start_price"`
	CurrentPrice     float64         `json:"current_price"`
	SignedMove       float64         `json:"signed_move"`
	AbsMove          float64         `json:"abs_move"`
	PeakVolumeRank   int             `json:"peak_volume_rank"`
	CurrentPriceRank int             `json:"current_price_rank"` // Equal to PeakPriceRank at session close
}

// =====================================================================
// CORE ENUM SPECIFICATIONS
// =====================================================================

type AnomalyType int

const (
	AnomalyNone        AnomalyType = 0
	AnomalyVolumeBurst AnomalyType = 1
	AnomalyAbsorption  AnomalyType = 2
)

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

func (a AnomalyType) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf("%q", a.String())), nil
}

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

type RegimeDirection int

const (
	DirectionFlat    RegimeDirection = 0
	DirectionBullish RegimeDirection = 1
	DirectionBearish RegimeDirection = -1
)

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
