package pipeline

import (
	"math"
	"time"
)

const (
	ContinuousWindowDuration = 60 * time.Second
)

// EnrichmentTick handles single localized data points inside the rolling collection.
type EnrichmentTick struct {
	Timestamp time.Time
	Volume    float64
	Price     float64
}

// TokenRollingBuffer encapsulates performance-optimized rolling metrics state management.
type TokenRollingBuffer struct {
	Ticks []EnrichmentTick

	// Continuous Window Extreme High/Low Boundaries
	RollingHigh float64
	RollingLow  float64

	// Live Accumulator Metrics
	AccumulatedVolume float64
	AccumulatedTicks  int
}

// NewTokenRollingBuffer initializes a buffer with an optimized underlying array allocation.
func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks:       make([]EnrichmentTick, 0, 1000),
		RollingHigh: -1.0,
		RollingLow:  math.MaxFloat64,
	}
}

// Push incorporates incoming tick variants and enforces sliding time-duration evictions.
func (b *TokenRollingBuffer) Push(ts time.Time, price, vol float64, duration time.Duration) {
	b.AccumulatedVolume += vol
	b.AccumulatedTicks++

	b.Ticks = append(b.Ticks, EnrichmentTick{
		Timestamp: ts,
		Volume:    vol,
		Price:     price,
	})

	// Evict expiration records exceeding time constraint boundaries
	cutoff := ts.Add(-duration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}
	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}

	// Recalculate structural extreme values over the active window context
	b.RollingHigh = -1.0
	b.RollingLow = math.MaxFloat64
	for _, t := range b.Ticks {
		if t.Price > b.RollingHigh {
			b.RollingHigh = t.Price
		}
		if t.Price < b.RollingLow {
			b.RollingLow = t.Price
		}
	}
}

// GetStats returns the rolling aggregate volume and sample count inside the current active window.
func (b *TokenRollingBuffer) GetStats() (float64, int) {
	var totalVol float64
	for _, t := range b.Ticks {
		totalVol += t.Volume
	}
	return totalVol, len(b.Ticks)
}
