package models

import "time"

type AnomalySnapshot struct {
	Timestamp  time.Time   `json:"ts"`
	Type       AnomalyType `json:"type"` //
	Direction  int         `json:"dir"`  // -1 = Sell, 1 = Buy
	VolumeRank int         `json:"vol_rank"`
	PriceRank  int         `json:"price_rank"`
	Price      float64     `json:"price"`
}

type AbsorptionLevel struct {
	Price             float64 `json:"price"` // The original point of passive validation (Equilibrium)
	Direction         int     `json:"dir"`   // 1 = Support (Passive Buy), -1 = Resistance (Passive Sell)
	Strength          int     `json:"strength"`
	IsActive          bool    `json:"is_active"`
	TearBoundary      float64 `json:"tear_boundary"`       // Absolute physical price boundary where membrane fails
	MaxStretchedPrice float64 `json:"max_stretched_price"` // Records deep testing extensions for chart analysis
}

type PeakAnomalyMetrics struct {
	PeakVolumeRank int `json:"peak_volume_rank"`
	PeakPriceRank  int `json:"peak_price_rank"`
	PeakTickRank   int `json:"peak_tick_rank"`
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

	TotalBuyQty  float64    `json:"total_buy_qty"`
	TotalSellQty float64    `json:"total_sell_qty"`
	ChangePct    float64    `json:"change_pct"`
	Ticks        []TickData `json:"-"`
}
