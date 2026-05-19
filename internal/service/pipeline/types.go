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

// RollingRegression maintains O(1) sufficient statistics for linear regression.
type RollingRegression struct {
	N     float64
	SumX  float64
	SumY  float64
	SumXY float64
	SumX2 float64
}

// Add incorporates a new data point into the running totals.
func (r *RollingRegression) Add(x, y float64) {
	r.N++
	r.SumX += x
	r.SumY += y
	r.SumXY += (x * y)
	r.SumX2 += (x * x)
}

// Remove takes an expiring data point out of the running totals.
func (r *RollingRegression) Remove(x, y float64) {
	if r.N <= 0 {
		return // Safety check
	}
	r.N--
	r.SumX -= x
	r.SumY -= y
	r.SumXY -= (x * y)
	r.SumX2 -= (x * x)
}

// Slope calculates the current geometric trajectory.
func (r *RollingRegression) Slope() float64 {
	if r.N < 2 {
		return 0.0 // Need at least 2 points to draw a line
	}

	denominator := (r.N * r.SumX2) - (r.SumX * r.SumX)
	if denominator == 0 {
		return 0.0 // Prevent divide-by-zero on flat vertical lines
	}

	return ((r.N * r.SumXY) - (r.SumX * r.SumY)) / denominator
}
