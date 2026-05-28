package models

import "time"

type MarketDNA struct {
	InstrumentToken     uint32
	StockName           string
	TradingDate         time.Time
	POC                 float64
	VAH                 float64
	VAL                 float64
	MacroHVNs           []VPExtrema
	MacroLVNs           []VPExtrema
	TimeBuckets         []TimeBucketDNA
	IntervalPercentiles map[string]PercentileThresholds
}

type PercentileThresholds struct {
	PriceP05 float64 `json:"price_p05"`
	PriceP10 float64 `json:"price_p10"`
	PriceP25 float64 `json:"price_p25"`
	PriceP50 float64 `json:"price_p50"`
	PriceP75 float64 `json:"price_p75"`
	PriceP90 float64 `json:"price_p90"`
	PriceP97 float64 `json:"price_p97"`

	RangeP05 float64 `json:"range_p05"`
	RangeP10 float64 `json:"range_p10"`
	RangeP25 float64 `json:"range_p25"`
	RangeP50 float64 `json:"range_p50"`
	RangeP75 float64 `json:"range_p75"`
	RangeP90 float64 `json:"range_p90"`
	RangeP97 float64 `json:"range_p97"`
}

type TimeBucketDNA struct {
	MinuteIndex     int `json:"minute_index"`
	SampleCount     int `json:"sample_count"`
	TickSampleCount int `json:"tick_sample_count"`

	// Volume metrics
	VolumeP05  float64 `json:"volume_p05"`
	VolumeP10  float64 `json:"volume_p10"`
	VolumeP25  float64 `json:"volume_p25"`
	VolumeP50  float64 `json:"volume_p50"`
	VolumeP75  float64 `json:"volume_p75"`
	VolumeP90  float64 `json:"volume_p90"`
	VolumeP97  float64 `json:"volume_p97"`
	VolumeStd  float64 `json:"volume_std"`
	VolumeMean float64 `json:"volume_mean"`

	// Tick Count metrics
	TickCountP05  float64 `json:"tick_count_p05"`
	TickCountP10  float64 `json:"tick_count_p10"`
	TickCountP25  float64 `json:"tick_count_p25"`
	TickCountP50  float64 `json:"tick_count_p50"`
	TickCountP75  float64 `json:"tick_count_p75"`
	TickCountP90  float64 `json:"tick_count_p90"`
	TickCountP97  float64 `json:"tick_count_p97"`
	TickCountStd  float64 `json:"tick_count_std"`
	TickCountMean float64 `json:"tick_count_mean"`
}
