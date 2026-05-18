// internal/service/pipeline/enrichment.go

package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type EnrichmentTick struct {
	Timestamp time.Time
	Price     float64
	Volume    float64
}

type TokenRollingBuffer struct {
	Ticks []EnrichmentTick
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks: make([]EnrichmentTick, 0, 500),
	}
}

func (b *TokenRollingBuffer) Push(ts time.Time, price float64, vol float64, duration time.Duration) {
	b.Ticks = append(b.Ticks, EnrichmentTick{
		Timestamp: ts,
		Price:     price,
		Volume:    vol,
	})

	cutoff := ts.Add(-duration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}
	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}
}

func (b *TokenRollingBuffer) GetMetrics() (float64, int) {
	var totalVol float64
	for _, t := range b.Ticks {
		totalVol += t.Volume
	}
	return totalVol, len(b.Ticks)
}

type EnrichmentStage struct {
	lastVolumeMap   map[uint32]int64
	lastPriceMap    map[uint32]float64
	positionManager order.PositionManager

	loc     *time.Location
	dnaMap  map[uint32]*models.MarketDNA
	buffers map[uint32]*TokenRollingBuffer

	mu sync.RWMutex
}

func NewEnrichmentStage(pm order.PositionManager) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &EnrichmentStage{
		lastVolumeMap:   make(map[uint32]int64),
		lastPriceMap:    make(map[uint32]float64),
		positionManager: pm,
		loc:             loc,
		dnaMap:          make(map[uint32]*models.MarketDNA),
		buffers:         make(map[uint32]*TokenRollingBuffer),
	}
}

func (s *EnrichmentStage) UpdateDNAMap(dnaMap map[uint32]*models.MarketDNA) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dnaMap = dnaMap
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp

	// 1. Calculate delta tick volume
	tick.TickVolume = s.calculateTickVolume(token, tick)
	vol := float64(tick.TickVolume)

	// Skip dead updates
	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	priceChanged := price != s.lastPriceMap[token]
	if priceChanged && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, price, ts)
	}
	s.lastPriceMap[token] = price

	// 2. Update continuous lookback buffer
	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}
	buf.Push(ts, price, vol, 60*time.Second)

	// 3. Compute pure statistics using Market DNA maps
	dna := s.dnaMap[token]
	var volumeZ float64
	var tickCountZ float64
	hasValidBaseline := false

	liveVolume, liveTickCount := buf.GetMetrics()

	if dna != nil {
		localTime := ts.In(s.loc)
		marketOpen := time.Date(localTime.Year(), localTime.Month(), localTime.Day(), 9, 15, 0, 0, s.loc)
		minuteIdx := int(localTime.Sub(marketOpen).Minutes())

		if minuteIdx >= 0 && minuteIdx < len(dna.TimeBuckets) {
			bucket := dna.TimeBuckets[minuteIdx]
			hasValidBaseline = true

			if bucket.VolumeStd > 0 {
				volumeZ = (liveVolume - bucket.VolumeMean) / bucket.VolumeStd
			}
			if bucket.TickCountStd > 0 {
				tickCountZ = (float64(liveTickCount) - bucket.TickCountMean) / bucket.TickCountStd
			}
		}
	}

	// 4. Attach pure descriptive metrics to pipeline context
	tick.VolumeZ = volumeZ
	tick.TickCountZ = tickCountZ
	tick.LiveVolume = liveVolume
	tick.LiveTickCount = liveTickCount
	tick.HasBaseline = hasValidBaseline

	// Package a lightweight copy of the window timeline context for downstream usage
	tick.WindowTicks = make([]models.WindowTick, len(buf.Ticks))
	for i, t := range buf.Ticks {
		tick.WindowTicks[i] = models.WindowTick{
			Price:  t.Price,
			Volume: t.Volume,
		}
	}

	return nil
}

func (s *EnrichmentStage) calculateTickVolume(token uint32, tick *models.EnrichedTick) int64 {
	curr := tick.Raw.CumulativeVolume
	prev := s.lastVolumeMap[token]

	var delta int64
	switch {
	case prev == 0:
		delta = tick.Raw.LastTradedQuantity
	case curr >= prev:
		delta = curr - prev
	default:
		delta = tick.Raw.LastTradedQuantity
	}

	s.lastVolumeMap[token] = curr
	return delta
}
