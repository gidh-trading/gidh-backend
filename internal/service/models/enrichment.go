package models

import "time"

// 1. Live Telemetry State (Pure, Un-normalized Measurements)
type LiveTelemetry struct {
	MinuteIndex        int     `json:"minute_index"`
	Volume             float64 `json:"volume"`
	TickCount          int64   `json:"tick_count"`
	RelativeVolume     float64 `json:"relative_volume"`
	RealizedRange      float64 `json:"realized_range"`
	RealizedVolatility float64 `json:"realized_volatility"`
	Efficiency         float64 `json:"efficiency"`
}

// 2. Enrichment State (Live Telemetry Projected against Historical DNA)
type EnrichmentState struct {
	MinuteIndex int `json:"minute_index"`

	// Participation normalization (Z-Scores)
	VolumeZ         float64 `json:"volume_z"`
	TickZ           float64 `json:"tick_z"`
	RelativeVolumeZ float64 `json:"relative_volume_z"`

	// Response percentile states (Non-Gaussian Strings)
	RangePercentile      string `json:"range_pct"`
	EfficiencyPercentile string `json:"efficiency_pct"`

	// Composite anomaly scores
	ParticipationScore float64 `json:"participation_score"`

	// State flags (Boolean Classifiers)
	IsVolumeExtreme bool `json:"is_volume_extreme"`
	IsTickExtreme   bool `json:"is_tick_extreme"`
	IsRangeExtreme  bool `json:"is_range_extreme"`

	Timestamp time.Time `json:"timestamp"`
}

// 3. The Enriched Tick Wrapper
type EnrichedTick struct {
	Raw            TickData           `json:"raw"`
	TickVolume     int64              `json:"-"`
	VolProfile     *VolumeProfileInfo `json:"vol_profile,omitempty"`
	FullVolProfile *VolumeProfile     `json:"full_vol_profile,omitempty"`

	// Clean architectural separation
	Telemetry  LiveTelemetry   `json:"telemetry"`
	Enrichment EnrichmentState `json:"enrichment"`

	MinuteIndex    int    `json:"minute_index"`
	DNASampleCount int    `json:"dna_sample_count"`
	EnrichmentStr  string `json:"enrichment_str"`
	EnrichedAt     int64  `json:"enriched_at"`
}
