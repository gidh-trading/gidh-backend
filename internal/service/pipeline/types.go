package pipeline

import (
	"time"
)

type rollingEntry struct {
	ts    time.Time
	price float64
	vol   float64
}

type RollingState struct {
	queue   []rollingEntry
	Volume  float64
	Open    float64
	High    float64
	Low     float64
	Close   float64
	LastDir int
}

type SessionState struct {
	TotalVolume float64
	TotalRange  float64
	Count       int
}

// DirectionalEfficiency calculates trend quality over the rolling 60s window.
// It uses Net Displacement vs True Range to filter out intraday bid-ask noise.
// Returns -1.0 (perfect downtrend) to 1.0 (perfect uptrend).
func (r *RollingState) DirectionalEfficiency() float64 {
	// If price hasn't moved at all, efficiency is 0
	if r.High == r.Low {
		return 0.0
	}

	// Numerator: Net Displacement (Close - Open) keeps the +/- sign
	netDisplacement := r.Close - r.Open

	// Denominator: The total extreme range of the 60-second window
	totalRange := r.High - r.Low

	return netDisplacement / totalRange
}
