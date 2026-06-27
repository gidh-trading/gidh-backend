package models

import "time"

// internal/service/models/bar.go

type BarAnalytics struct {
	VolumeRank     int            `json:"volume_rank"`
	TickRank       int            `json:"tick_rank"`
	PriceRank      int            `json:"price_rank"`
	EfficiencyRank int            `json:"efficiency_rank"` // Added
	RangeRank      int            `json:"range_rank"`
	Direction      DirectionState `json:"direction"`
	UpperWickRank  int            `json:"upper_wick_rank"`
	LowerWickRank  int            `json:"lower_wick_rank"`

	// --- Independent Rolling Window Baselines ---
	RollingVolumeIntensity float64 `json:"rolling_volume_intensity"`
	RollingPriceNormalized float64 `json:"rolling_price_normalized"`
	RollingTickRank        float64 `json:"rolling_tick_rank"`
	RollingEfficiencyRank  float64 `json:"rolling_efficiency_rank"` // Added
	RollingMomentumScore   float64 `json:"rolling_momentum_score"`  // Added

	RollingFlowIntensity float64 `json:"rolling_flow_intensity"` // 🟢 NEW Composite
	RollingExecutionEdge float64 `json:"rolling_execution_edge"` // 🟢 NEW Composite

	// --- Independent 1-Minute Directional Slopes ---
	VolumeSlope     float64 `json:"volume_slope"`
	PriceSlope      float64 `json:"price_slope"`
	TickSlope       float64 `json:"tick_slope"`
	EfficiencySlope float64 `json:"efficiency_slope"` // Added
	MomentumSlope   float64 `json:"momentum_slope"`   // Added

	NormalizedVwapDistance float64 `json:"normalized_vwap_distance"`
	VwapClosePct           float64 `json:"vwap_close_pct"`
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
