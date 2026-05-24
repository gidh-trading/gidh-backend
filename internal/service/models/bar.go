package models

import "time"

type AnomalySnapshot struct {
	Timestamp  time.Time   `json:"ts"`
	Type       AnomalyType `json:"type"` //
	Direction  int         `json:"dir"`  // -1 = Sell, 1 = Buy
	VolumeRank int         `json:"vol_rank"`
	PriceRank  int         `json:"price_rank"`
}

// PeakAnomalyMetrics remains a strict Go struct for compiler safety,
// but serializes out as a flat JSON dictionary object for database/sockets.
type PeakAnomalyMetrics struct {
	PeakVolumeRank      int `json:"peak_volume_rank"`
	PeakPriceRank       int `json:"peak_price_rank"`
	PeakTickRank        int `json:"peak_tick_rank"`
	MaxAnomalyDirection int `json:"max_anomaly_direction"`
	MaxAbsorptionSignal int `json:"max_absorption_signal"`
}

type Bar struct {
	Timestamp       time.Time `json:"timestamp"`
	InstrumentToken int32     `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	Timeframe       string    `json:"timeframe"`

	// ---- Pure OHLC ----
	Open  float64 `json:"open"`
	High  float64 `json:"high"`
	Low   float64 `json:"low"`
	Close float64 `json:"close"`

	// ---- Aggregated Quantities ----
	Volume    float64 `json:"volume"`
	TickCount int64   `json:"tick_count"`
	VWAP      float64 `json:"vwap"`

	// ---- Auction Framework Elements ----
	POC float64 `json:"poc"`
	VAH float64 `json:"vah"`
	VAL float64 `json:"val"`

	// ---- Dynamic Structural Strategy Blocks ----
	Peaks             PeakAnomalyMetrics `json:"peaks"`
	SignificantEvents []AnomalySnapshot  `json:"significant_events,omitempty"`

	TotalBuyQty  float64    `json:"total_buy_qty"`
	TotalSellQty float64    `json:"total_sell_qty"`
	ChangePct    float64    `json:"change_pct"`
	Ticks        []TickData `json:"-"`
}
