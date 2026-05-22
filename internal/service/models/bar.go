package models

import "time"

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

	// ---- Volume ----
	Volume float64 `json:"volume"`

	// ---- Tick Activity (NEW) ----
	TickCount     int64   `json:"tick_count"`
	MaxTickCountZ float64 `json:"max_tick_count_z"`

	// ---- Optional Auction Metrics ----
	VWAP float64 `json:"vwap"`
	POC  float64 `json:"poc"`
	VAH  float64 `json:"vah"`
	VAL  float64 `json:"val"`

	TotalBuyQty  float64 `json:"total_buy_qty"`
	TotalSellQty float64 `json:"total_sell_qty"`
	ChangePct    float64 `json:"change_pct"`

	// ---- UI Only Metrics (Not persisted in DB) ----
	UnrealizedPnL float64 `json:"unrealized_pnl"`

	Ticks []TickData `json:"-"`
}
