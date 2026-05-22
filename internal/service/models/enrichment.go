// internal/service/models/domain.go

package models

type ParticipationMetrics struct {
	TickVolume      int64   `json:"tick_volume"`
	TickCount       int64   `json:"tick_count"`
	VolumeZ         float64 `json:"volume_z"`
	TickCountZ      float64 `json:"tick_count_z"`
	RelativeVolumeZ float64 `json:"relative_volume_z"`
}

type ResponseMetrics struct {
	RealizedRange      float64 `json:"realized_range"`
	RealizedVolatility float64 `json:"realized_volatility"`
	Efficiency         float64 `json:"efficiency"`
	EfficiencyPct      float64 `json:"efficiency_pct"` // Non-Gaussian empirical distribution percentile rank (0-100)
}

type EnrichedTick struct {
	Raw            TickData           `json:"raw"`
	TickVolume     int64              `json:"-"` // Retained for fast pipeline routing step checks
	VolProfile     *VolumeProfileInfo `json:"vol_profile,omitempty"`
	FullVolProfile *VolumeProfile     `json:"full_vol_profile,omitempty"`

	// Production Metric Sub-structures
	Participation ParticipationMetrics `json:"participation"`
	Response      ResponseMetrics      `json:"response"`

	MinuteIndex    int   `json:"minute_index"`
	DNASampleCount int64 `json:"dna_sample_count"`
	EnrichedAt     int64 `json:"enriched_at"`
}
