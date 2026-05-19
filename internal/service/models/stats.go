package models

import "time"

type MarketDNA struct {
	InstrumentToken uint32
	StockName       string
	TradingDate     time.Time
	POC             float64
	VAH             float64
	VAL             float64
	MacroHVNs       []VPExtrema
	MacroLVNs       []VPExtrema
	TimeBuckets     []TimeBucketDNA
}

type TimeBucketDNA struct {
	MinuteIndex   int     `json:"minute_index"`
	VolumeMean    float64 `json:"volume_mean"`
	VolumeStd     float64 `json:"volume_std"`
	RangeMean     float64 `json:"range_mean"`
	RangeStd      float64 `json:"range_std"`
	TickCountMean float64 `json:"tick_count_mean"`
	TickCountStd  float64 `json:"tick_count_std"`
}

type TradeStats struct {
	// --- Time context ---
	MinuteIndex int
	Timestamp   time.Time

	// --- Rolling candle stats ---
	Volume1m float64
	Range1m  float64

	// --- Session context ---
	SessionVolume   float64
	SessionAvgRange float64

	// --- Normalized features (must match DNA logic) ---
	NormVolume float64
	NormRange  float64

	// --- DNA reference (optional but useful for debugging) ---
	VolumeMean float64
	VolumeStd  float64
	RangeMean  float64
	RangeStd   float64

	// --- Z-scores (core signal inputs) ---
	VolumeZ float64
	RangeZ  float64

	// Live Energy accumulation
	TotalVolEnergy float64 `json:"total_vol_energy"`
	BuyVolEnergy   float64 `json:"buy_vol_energy"`
	SellVolEnergy  float64 `json:"sell_vol_energy"`

	TotalRngEnergy float64 `json:"total_rng_energy"`
	BuyRngEnergy   float64 `json:"buy_rng_energy"`
	SellRngEnergy  float64 `json:"sell_rng_energy"`
}

type UIHeatmapCell struct {
	P float64 `json:"p"` // Price Bin (Bucket)
	V float64 `json:"v"` // Total Volume in this bucket
	D float64 `json:"d"` // Trade Delta (Aggressive Buy - Aggressive Sell)
	I float64 `json:"i"` // Intensity (Count of anomaly ticks in this bucket)
}

type TickMicrostructure struct {
	AggressiveBuy  float64
	AggressiveSell float64
}

type HeatmapCell struct {
	PriceBin       float64
	CellVolume     float64
	AggressiveBuy  float64
	AggressiveSell float64
	IntensityScore float64
}
