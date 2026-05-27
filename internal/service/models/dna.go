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

	// Price / Volatility metrics
	VolatilityP05 float64 `json:"volatility_p05"`
	VolatilityP10 float64 `json:"volatility_p10"`
	VolatilityP25 float64 `json:"volatility_p25"`
	VolatilityP50 float64 `json:"volatility_p50"`
	VolatilityP75 float64 `json:"volatility_p75"`
	VolatilityP90 float64 `json:"volatility_p90"`
	VolatilityP97 float64 `json:"volatility_p97"`
}
