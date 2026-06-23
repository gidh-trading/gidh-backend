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

	// --- Continuous Heatmap Visualizers ---
	VolumeIntensity       float64 `json:"volume_intensity"`        // Multiplier of ADV30D (e.g., 0.5 to 10.0+)
	PriceNormalizedChange float64 `json:"price_normalized_change"` // Interval-bounded progress (-1.0 to +1.0)

	// --- Running Continuous Accumulators ---
	ContinuousVolumeIntensity float64 `json:"continuous_volume_intensity"` // Compounding volume pressure (0.0 to +Inf)
	ContinuousPriceNormalized float64 `json:"continuous_price_normalized"` // Compounding price momentum (-5.0 to +5.0)

	// --- Structural Rank Blends ---
	Convergence float64 `json:"convergence"` // (VolumeRank + PriceRank) / 2
	Divergence  float64 `json:"divergence"`  // (VolumeRank - PriceRank) / 2

	// Retained helper distances if needed by structural components
	NormalizedVwapDistance float64 `json:"normalized_vwap_distance"`
	TimePctAboveVwap       float64 `json:"time_pct_above_vwap"`
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
