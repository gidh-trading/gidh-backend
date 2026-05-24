package pipeline

import (
	"time"
)

const (
	ContinuousWindowDuration = 60 * time.Second
)

type WindowTick struct {
	Timestamp time.Time
	Volume    float64
	Price     float64
}

type TokenRollingBuffer struct {
	Ticks     []WindowTick
	lastPrice float64
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		// Pre-allocate capacity for roughly 20 ticks/sec over 60 seconds
		Ticks: make([]WindowTick, 0, 1200),
	}
}

// Push adds a new tick and evicts data outside the 60-second rolling context
func (b *TokenRollingBuffer) Push(ts time.Time, price, vol float64) {
	b.lastPrice = price

	b.Ticks = append(b.Ticks, WindowTick{
		Timestamp: ts,
		Volume:    vol,
		Price:     price,
	})

	// Evict metrics outside our sliding 60s window context
	cutoff := ts.Add(-ContinuousWindowDuration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}

	// Shift the slice to drop evicted ticks and free memory
	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}
}

// GetProductionMetrics computes precise time-series indicators for the 60s window
func (b *TokenRollingBuffer) GetProductionMetrics() (vol float64, count int64, displacement float64) {
	if len(b.Ticks) == 0 {
		return 0, 0, 0
	}

	var totalVol float64

	// Accurately sum volume over the surviving 60-second window
	for _, t := range b.Ticks {
		totalVol += t.Volume
	}

	// Displacement is simply the Last Price (Close) minus the First Price in the window (Open)
	windowOpen := b.Ticks[0].Price
	windowClose := b.lastPrice
	displacement = windowClose - windowOpen

	return totalVol, int64(len(b.Ticks)), displacement
}
