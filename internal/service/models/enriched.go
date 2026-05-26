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

// SessionSnapshot holds the raw statistical facts committed at each minute checkpoint
type SessionSnapshot struct {
	Timestamp    time.Time `json:"timestamp"`
	MinuteIndex  int       `json:"minute_index"`
	VolumeRank   int       `json:"volume_rank"`  // Linearized 1-7 coordinate space
	PriceRank    int       `json:"price_rank"`   // Linearized 1-7 coordinate space
	Displacement float64   `json:"displacement"` // Raw fact: Close - Open over the 60s window
	ClosePrice   float64   `json:"close_price"`
}

// SessionContext tracks the continuous chronological array canvas for an instrument
type SessionContext struct {
	Timeline       []SessionSnapshot
	MaxStoredSteps int
}

// CalculateCumulativePressure aggregates your sustained lookback metrics mathematically
func (sc *SessionContext) CalculateCumulativePressure(lookbackMinutes int) (energySum int, netDisplacement float64, stepsEvaluated int) {
	totalSteps := len(sc.Timeline)
	if totalSteps == 0 {
		return 0, 0, 0
	}

	startIdx := totalSteps - lookbackMinutes
	if startIdx < 0 {
		startIdx = 0
	}

	for i := startIdx; i < totalSteps; i++ {
		step := sc.Timeline[i]
		// Values above baseline coordinate 4 (P50) represent abnormal participation size
		if step.VolumeRank >= 5 {
			energySum += (step.VolumeRank - 4)
		}
		netDisplacement += step.Displacement
		stepsEvaluated++
	}

	return energySum, netDisplacement, stepsEvaluated
}
