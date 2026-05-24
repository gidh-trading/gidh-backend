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
		Ticks: make([]WindowTick, 0, 1200),
	}
}

func (b *TokenRollingBuffer) Push(ts time.Time, price, vol float64) {
	b.lastPrice = price

	b.Ticks = append(b.Ticks, WindowTick{
		Timestamp: ts,
		Volume:    vol,
		Price:     price,
	})

	cutoff := ts.Add(-ContinuousWindowDuration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}

	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}
}

// GetProductionMetrics computes precise time-series indicators for the 60s window
func (b *TokenRollingBuffer) GetProductionMetrics() (vol float64, count int64, displacement float64) {
	if len(b.Ticks) == 0 {
		return 0, 0, 0
	}

	// Accurately sum volume over the surviving 60-second window
	for _, t := range b.Ticks {
		vol += t.Volume
	}

	// Displacement is simply the Last Price (Close) minus the First Price in the window (Open)
	windowOpen := b.Ticks[0].Price
	windowClose := b.lastPrice
	displacement = windowClose - windowOpen

	return vol, int64(len(b.Ticks)), displacement
}

// GetProductionStructure extracts pure, ungameable committed capital structural metrics
func (b *TokenRollingBuffer) GetProductionStructure() (vol float64, rOpen, rHigh, rLow, rClose float64) {
	if len(b.Ticks) == 0 {
		return 0, 0, 0, 0, 0
	}

	firstTick := b.Ticks[0]
	rOpen = firstTick.Price
	rHigh = firstTick.Price
	rLow = firstTick.Price
	rClose = b.lastPrice

	for _, t := range b.Ticks {
		vol += t.Volume
		if t.Price > rHigh {
			rHigh = t.Price
		}
		if t.Price < rLow {
			rLow = t.Price
		}
	}

	return vol, rOpen, rHigh, rLow, rClose
}

// GetRollingStructure fetches the current 60s window structural prices for an instrument token
func (s *EnrichmentStage) GetRollingStructure(token uint32) (vol, rOpen, rHigh, rLow, rClose float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, exists := s.instruments[token]
	if !exists || ctx.Buffer == nil {
		return 0, 0, 0, 0, 0
	}

	return ctx.Buffer.GetProductionStructure()
}
