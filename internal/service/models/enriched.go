package models

import "time"

type LiveTelemetry struct {
	MinuteIndex      int     `json:"minute_index"`
	LiveVolume       float64 `json:"live_volume"`
	LiveDisplacement float64 `json:"live_displacement"` // Replaces RealizedRange (Close - Open)
	TickCount        int64   `json:"tick_count"`
}

type EnrichmentState struct {
	MinuteIndex int `json:"minute_index"`

	// Non-Gaussian Percentile Strings straight from DNA matching
	VolumePercentile string `json:"volume_pct"`
	PricePercentile  string `json:"price_pct"` // Maps to live_displacement
	TickPercentile   string `json:"tick_pct"`

	Timestamp time.Time `json:"timestamp"`
}

type EnrichedTick struct {
	Raw            TickData           `json:"raw"`
	TickVolume     int64              `json:"-"`
	VolProfile     *VolumeProfileInfo `json:"vol_profile,omitempty"`
	FullVolProfile *VolumeProfile     `json:"full_vol_profile,omitempty"`

	Telemetry  LiveTelemetry   `json:"telemetry"`
	Enrichment EnrichmentState `json:"enrichment"`

	MinuteIndex    int   `json:"minute_index"`
	DNASampleCount int   `json:"dna_sample_count"`
	EnrichedAt     int64 `json:"enriched_at"`
}
