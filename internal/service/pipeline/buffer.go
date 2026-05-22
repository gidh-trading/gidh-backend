package pipeline

import (
	"math"
	"time"
)

const (
	ContinuousWindowDuration = 60 * time.Second
)

type WindowTick struct {
	Timestamp time.Time
	Volume    float64
	Price     float64
	LogReturn float64
}

type TokenRollingBuffer struct {
	Ticks             []WindowTick
	AccumulatedVolume float64
	AccumulatedTicks  int
	RollingHigh       float64
	RollingLow        float64
	lastPrice         float64
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks:       make([]WindowTick, 0, 1200),
		RollingHigh: -1.0,
		RollingLow:  math.MaxFloat64,
	}
}

func (b *TokenRollingBuffer) Push(ts time.Time, price, vol float64) {
	b.AccumulatedVolume += vol
	b.AccumulatedTicks++

	logRet := 0.0
	if b.lastPrice > 0 && price > 0 {
		logRet = math.Log(price / b.lastPrice)
	}
	b.lastPrice = price

	b.Ticks = append(b.Ticks, WindowTick{
		Timestamp: ts,
		Volume:    vol,
		Price:     price,
		LogReturn: logRet,
	})

	// Evict metrics outside our sliding 60s window context
	cutoff := ts.Add(-ContinuousWindowDuration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}
	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}

	// Dynamic window parameters calculation pass
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

// GetProductionMetrics computes precise time-series indicators for the spec
func (b *TokenRollingBuffer) GetProductionMetrics() (vol float64, count int64, rnge float64, volty float64) {
	if len(b.Ticks) == 0 {
		return 0, 0, 0, 0
	}

	var totalVol float64
	var sumSqLogRet float64

	for _, t := range b.Ticks {
		totalVol += t.Volume
		sumSqLogRet += t.LogReturn * t.LogReturn
	}

	rnge = 0.0
	if b.RollingHigh > 0 && b.RollingLow < math.MaxFloat64 {
		rnge = b.RollingHigh - b.RollingLow
	}

	// Realized Volatility = sqrt(sum(log_return²))
	volty = math.Sqrt(sumSqLogRet)
	return totalVol, int64(len(b.Ticks)), rnge, volty
}
