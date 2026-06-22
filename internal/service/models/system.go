// internal/service/models/domain.go

package models

import "time"

// =====================================================================
// 1. SYSTEM & CONFIGURATION
// =====================================================================

type InstrumentConfig struct {
	Token      uint32 `json:"instrument_token"`
	Name       string `json:"stock_name"`
	IsBacktest bool   `json:"is_backtest"`
}

type InstrumentProfile struct {
	StockName       string  `json:"stock_name"`
	InstrumentToken uint32  `json:"instrument_token"`
	BucketSize      float64 `json:"bucket_size"`
	ATR14           float64 `json:"atr_14"`
	ADRPct          float64 `json:"adr_pct"`
	ADV30d          int64   `json:"adv_30d"`
	ADVVal30d       float64 `json:"adv_val_30d"`
}

// VWAPDistancePercentile represents a row from the gidh_vwap_distance_percentiles table
type VWAPDistancePercentile struct {
	InstrumentToken uint32    `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	TradingDate     time.Time `json:"trading_date"`

	// Positive Extensions Pool (Price >= VWAP)
	PosP50 float64 `json:"pos_p50"`
	PosP75 float64 `json:"pos_p75"`
	PosP90 float64 `json:"pos_p90"`
	PosP97 float64 `json:"pos_p97"`
	PosP99 float64 `json:"pos_p99"`

	// Negative Extensions Pool (Price < VWAP, stored as absolute magnitude)
	NegP50 float64 `json:"neg_p50"`
	NegP75 float64 `json:"neg_p75"`
	NegP90 float64 `json:"neg_p90"`
	NegP97 float64 `json:"neg_p97"`
	NegP99 float64 `json:"neg_p99"`
}
