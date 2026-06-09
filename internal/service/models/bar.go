package models

import "time"

type BarAnalytics struct {
	VolumeRank    int            `json:"volume_rank"`
	TickRank      int            `json:"tick_rank"`
	PriceRank     int            `json:"price_rank"`
	RangeRank     int            `json:"range_rank"`
	Direction     DirectionState `json:"direction"`
	UpperWickRank int            `json:"upper_wick_rank"`
	LowerWickRank int            `json:"lower_wick_rank"`
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

	TotalBuyQty  float64 `json:"total_buy_qty"`
	TotalSellQty float64 `json:"total_sell_qty"`
	ChangePct    float64 `json:"change_pct"`

	// Analytical Metadata Container
	Analytics BarAnalytics `json:"analytics"`

	Ticks []TickData `json:"-"`
}
