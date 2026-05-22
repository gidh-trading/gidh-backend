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
	MinuteIndex     int `json:"minute_index"`
	SampleCount     int `json:"sample_count"`
	TickSampleCount int `json:"tick_sample_count"`

	// Range metrics
	RangeP05 float64 `json:"range_p05"`
	RangeP10 float64 `json:"range_p10"`
	RangeP50 float64 `json:"range_p50"`
	RangeP90 float64 `json:"range_p90"`
	RangeP95 float64 `json:"range_p95"`
	RangeP99 float64 `json:"range_p99"`

	// Volume metrics
	VolumeP05  float64 `json:"volume_p05"`
	VolumeP10  float64 `json:"volume_p10"`
	VolumeP50  float64 `json:"volume_p50"`
	VolumeP90  float64 `json:"volume_p90"`
	VolumeP95  float64 `json:"volume_p95"`
	VolumeP99  float64 `json:"volume_p99"`
	VolumeStd  float64 `json:"volume_std"`
	VolumeMean float64 `json:"volume_mean"`

	// Efficiency metrics
	EfficiencyP05 float64 `json:"efficiency_p05"`
	EfficiencyP10 float64 `json:"efficiency_p10"`
	EfficiencyP50 float64 `json:"efficiency_p50"`
	EfficiencyP90 float64 `json:"efficiency_p90"`
	EfficiencyP95 float64 `json:"efficiency_p95"`
	EfficiencyP99 float64 `json:"efficiency_p99"`

	// Tick Count metrics
	TickCountP05  float64 `json:"tick_count_p05"`
	TickCountP10  float64 `json:"tick_count_p10"`
	TickCountP50  float64 `json:"tick_count_p50"`
	TickCountP90  float64 `json:"tick_count_p90"`
	TickCountP95  float64 `json:"tick_count_p95"`
	TickCountP99  float64 `json:"tick_count_p99"`
	TickCountStd  float64 `json:"tick_count_std"`
	TickCountMean float64 `json:"tick_count_mean"`

	// Relative Volume metrics (Newly Added)
	RelativeVolumeP05  float64 `json:"relative_volume_p05"`
	RelativeVolumeP10  float64 `json:"relative_volume_p10"`
	RelativeVolumeP50  float64 `json:"relative_volume_p50"`
	RelativeVolumeP90  float64 `json:"relative_volume_p90"`
	RelativeVolumeP95  float64 `json:"relative_volume_p95"`
	RelativeVolumeP99  float64 `json:"relative_volume_p99"`
	RelativeVolumeStd  float64 `json:"relative_volume_std"`
	RelativeVolumeMean float64 `json:"relative_volume_mean"`
}

type UIHeatmapCell struct {
	P float64 `json:"p"` // Price Bin (Bucket)
	V float64 `json:"v"` // Total Volume in this bucket
	D float64 `json:"d"` // Trade Delta (Aggressive Buy - Aggressive Sell)
	I float64 `json:"i"` // Intensity (Count of anomaly ticks in this bucket)
}
