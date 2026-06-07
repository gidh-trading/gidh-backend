// internal/service/models/domain.go

package models

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
