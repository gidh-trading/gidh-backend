package models

import "time"

type LiveTelemetry struct {
	MinuteIndex    int     `json:"minute_index"`
	RelativeVolume float64 `json:"relative_volume"`
	RealizedRange  float64 `json:"realized_range"`
	TickCount      int64   `json:"tick_count"`
}

type EnrichmentState struct {
	MinuteIndex int `json:"minute_index"`

	// Non-Gaussian Percentile Strings straight from DNA
	VolumeZPercentile        string `json:"volume_z_pct"`
	RelativeVolumePercentile string `json:"relative_volume_pct"`
	RangePercentile          string `json:"range_pct"`
	TickPercentile           string `json:"tick_pct"`

	Timestamp time.Time `json:"timestamp"`
}

type EnrichedTick struct {
	Raw            TickData           `json:"raw"`
	TickVolume     int64              `json:"-"`
	VolProfile     *VolumeProfileInfo `json:"vol_profile,omitempty"`
	FullVolProfile *VolumeProfile     `json:"full_vol_profile,omitempty"`

	Telemetry  LiveTelemetry   `json:"telemetry"`
	Enrichment EnrichmentState `json:"enrichment"`

	MinuteIndex    int    `json:"minute_index"`
	DNASampleCount int    `json:"dna_sample_count"`
	EnrichmentStr  string `json:"enrichment_str"` // Still maps to relative volume string for BarManager
	EnrichedAt     int64  `json:"enriched_at"`
}
