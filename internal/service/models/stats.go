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
