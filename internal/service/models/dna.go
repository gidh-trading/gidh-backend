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
	// Intraday bucket identifier
	MinuteIndex int `json:"minute_index"`

	// --------------------------------------------------
	// PARTICIPATION DNA
	// --------------------------------------------------

	// Raw committed participation
	VolumeMean float64 `json:"volume_mean"`
	VolumeStd  float64 `json:"volume_std"`

	// Execution frequency participation
	TickCountMean float64 `json:"tick_count_mean"`
	TickCountStd  float64 `json:"tick_count_std"`

	// Relative participation behavior
	// (current volume vs expected bucket volume)
	RelativeVolumeMean float64 `json:"relative_volume_mean"`
	RelativeVolumeStd  float64 `json:"relative_volume_std"`

	// --------------------------------------------------
	// RESPONSE DNA
	// --------------------------------------------------

	// Raw realized response
	RangeMean float64 `json:"range_mean"`
	RangeStd  float64 `json:"range_std"`

	// Response-per-participation telemetry
	//
	// IMPORTANT:
	// We do NOT use mean/std normalization live
	// for efficiency currently.
	//
	// These are stored for:
	// - diagnostics
	// - distribution analysis
	// - research validation
	EfficiencyMean float64 `json:"efficiency_mean"`
	EfficiencyStd  float64 `json:"efficiency_std"`

	// --------------------------------------------------
	// ROBUST RESPONSE DISTRIBUTION
	// --------------------------------------------------

	// Recommended live normalization source
	// for EfficiencyPct calculations.
	EfficiencyP50 float64 `json:"efficiency_p50"`
	EfficiencyP90 float64 `json:"efficiency_p90"`
	EfficiencyP95 float64 `json:"efficiency_p95"`
	EfficiencyP99 float64 `json:"efficiency_p99"`

	// Optional:
	// useful for response percentile validation
	RangeP50 float64 `json:"range_p50"`
	RangeP90 float64 `json:"range_p90"`
	RangeP95 float64 `json:"range_p95"`
	RangeP99 float64 `json:"range_p99"`

	// --------------------------------------------------
	// SAMPLE STRENGTH
	// --------------------------------------------------

	// Number of historical observations used
	// to construct this bucket DNA.
	SampleCount int64 `json:"sample_count"`
}

type UIHeatmapCell struct {
	P float64 `json:"p"` // Price Bin (Bucket)
	V float64 `json:"v"` // Total Volume in this bucket
	D float64 `json:"d"` // Trade Delta (Aggressive Buy - Aggressive Sell)
	I float64 `json:"i"` // Intensity (Count of anomaly ticks in this bucket)
}
