package models

import "time"

type BarMetrics struct {
	PeakVolumeZRank int `json:"peak_volume_z_rank"`
	PeakPriceRank   int `json:"peak_price_rank"`
	PeakTickRank    int `json:"peak_tick_rank"`

	// --- New Production Strategy Metrics ---
	NormalizedDisplacement float64 `json:"normalized_displacement"`
	WickAsymmetry          float64 `json:"wick_asymmetry"`
	AnomalyDirection       int     `json:"anomaly_direction"` // -1 = Sell, 0 = No Anomaly, 1 = Buy
	AbsorptionSignal       int     `json:"absorption_signal"`
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
