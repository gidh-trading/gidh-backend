package models

import "time"

type BarMetrics struct {
	PeakVolumeZRank        int `json:"peak_volume_z_rank"`
	PeakRelativeVolumeRank int `json:"peak_relative_volume_rank"`
	PeakRangeRank          int `json:"peak_range_rank"`
	PeakTickRank           int `json:"peak_tick_rank"`
}

type Bar struct {
	Timestamp       time.Time `json:"timestamp"`
	InstrumentToken int32     `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	Timeframe       string    `json:"timeframe"`

	// ---- OHLC ----
	Open  float64 `json:"open"`
	High  float64 `json:"high"`
	Low   float64 `json:"low"`
	Close float64 `json:"close"`

	// ---- Aggregated Quantities ----
	Volume    float64 `json:"volume"`
	TickCount int64   `json:"tick_count"`

	// ---- Dynamic Metrics Block ----
	Metrics BarMetrics `json:"metrics"`

	// ---- Auction Framework Elements ----
	VWAP float64 `json:"vwap"`
	POC  float64 `json:"poc"`
	VAH  float64 `json:"vah"`
	VAL  float64 `json:"val"`

	TotalBuyQty  float64 `json:"total_buy_qty"`
	TotalSellQty float64 `json:"total_sell_qty"`
	ChangePct    float64 `json:"change_pct"`

	// ---- UI Only Local State ----
	UnrealizedPnL float64 `json:"unrealized_pnl"`

	Ticks []TickData `json:"-"`
}
