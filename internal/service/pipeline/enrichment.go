// internal/service/pipeline/enrichment.go
package pipeline

import (
	"gidh-backend/internal/service/order"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

// --- 1. Rolling Window Data Structures ---

type EnrichmentTick struct {
	Timestamp time.Time
	Volume    float64
	Price     float64 // NEW: Track price for regression
	VWAP      float64 // NEW: Track VWAP for regression
	X         float64 // NEW: Normalized time (seconds since midnight) to prevent float overflow
}

type TokenRollingBuffer struct {
	Ticks    []EnrichmentTick
	PriceReg RollingRegression
	VWAPReg  RollingRegression
	VolReg   RollingRegression
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks: make([]EnrichmentTick, 0, 500),
	}
}

func (b *TokenRollingBuffer) Push(ts time.Time, price, vwap, vol float64, duration time.Duration) {
	// Normalize X-axis to "seconds since midnight" for clean math
	x := float64(ts.Hour()*3600 + ts.Minute()*60 + ts.Second())

	tick := EnrichmentTick{
		Timestamp: ts,
		Volume:    vol,
		Price:     price,
		VWAP:      vwap,
		X:         x,
	}

	b.Ticks = append(b.Ticks, tick)

	// O(1) Add to regression running totals
	b.PriceReg.Add(x, price)
	b.VWAPReg.Add(x, vwap)
	b.VolReg.Add(x, vol)

	cutoff := ts.Add(-duration)
	evictIdx := 0
	for evictIdx < len(b.Ticks) && b.Ticks[evictIdx].Timestamp.Before(cutoff) {
		oldTick := b.Ticks[evictIdx]

		// O(1) Remove from regression running totals
		b.PriceReg.Remove(oldTick.X, oldTick.Price)
		b.VWAPReg.Remove(oldTick.X, oldTick.VWAP)
		b.VolReg.Remove(oldTick.X, oldTick.Volume)

		evictIdx++
	}
	if evictIdx > 0 {
		b.Ticks = b.Ticks[evictIdx:]
	}
}

// GetStats returns the total volume AND the number of ticks inside the rolling window
func (b *TokenRollingBuffer) GetStats() (float64, int) {
	var totalVol float64
	for _, t := range b.Ticks {
		totalVol += t.Volume
	}
	return totalVol, len(b.Ticks)
}

// --- 2. Enrichment Stage ---

type EnrichmentStage struct {
	lastVolumeMap   map[uint32]int64
	lastPriceMap    map[uint32]float64
	positionManager order.PositionManager
	advMap          map[uint32]float64
	dnaMap          map[uint32]map[int]models.TimeBucketDNA
	buffers         map[uint32]*TokenRollingBuffer
	loc             *time.Location
	mu              sync.Mutex
}

func NewEnrichmentStage(pm order.PositionManager, advMap map[uint32]float64, rawDnaMap map[uint32]*models.MarketDNA) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	// Pre-process the DNA arrays into an O(1) nested map for lightning-fast tick lookups
	fastDnaMap := make(map[uint32]map[int]models.TimeBucketDNA)
	for token, dna := range rawDnaMap {
		fastDnaMap[token] = make(map[int]models.TimeBucketDNA)
		for _, bucket := range dna.TimeBuckets {
			fastDnaMap[token][bucket.MinuteIndex] = bucket
		}
	}

	return &EnrichmentStage{
		lastVolumeMap:   make(map[uint32]int64),
		lastPriceMap:    make(map[uint32]float64),
		positionManager: pm,
		advMap:          advMap,
		dnaMap:          fastDnaMap,
		buffers:         make(map[uint32]*TokenRollingBuffer),
		loc:             loc,
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(s.loc) // Ensure time is strictly IST

	// 1. Calculate actual tick volume
	tick.TickVolume = s.calculateTickVolume(token, tick)
	vol := float64(tick.TickVolume)

	// Skip dead updates
	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	priceChanged := tick.Raw.LastPrice != s.lastPriceMap[token]

	if priceChanged && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}

	s.lastPriceMap[token] = price

	// 2. Maintain Continuous 60-Second Sliding Window
	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}
	// Feed the VWAP (AverageTradedPrice), Price, and Vol into the buffer
	buf.Push(ts, price, tick.Raw.AverageTradedPrice, vol, 60*time.Second)

	// Get live rolling stats
	liveVolume, liveTickCount := buf.GetStats()

	// 3. Compute Standard Relative Volume (RVol)
	var rVol float64
	if adv, ok := s.advMap[token]; ok && adv > 0 {
		expectedVolPerMin := adv / 375.0 // Indian market 375 mins
		if expectedVolPerMin > 0 {
			rVol = liveVolume / expectedVolPerMin
		}
	}
	tick.RelativeVolume = rVol

	// 4. Calculate Minute Index & DNA Z-Scores
	// 09:15 AM = (9*60)+15 = 555 minutes from midnight
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555

	var volZ, tcZ float64

	if tokenDna, exists := s.dnaMap[token]; exists {
		if baseline, ok := tokenDna[minuteIndex]; ok {

			// Volume Z-Score: (Live Vol - Historical Mean) / Historical StdDev
			if baseline.VolumeStd > 0 {
				volZ = (liveVolume - baseline.VolumeMean) / baseline.VolumeStd
			}

			// Tick Count Z-Score: (Live Ticks - Historical Mean) / Historical StdDev
			if baseline.TickCountStd > 0 {
				tcZ = (float64(liveTickCount) - baseline.TickCountMean) / baseline.TickCountStd
			}
		}
	}

	tick.VolumeZ = volZ
	tick.TickCountZ = tcZ
	pSlope, vSlope, volSlope := buf.GetMicroSlopes()
	tick.MicroPriceSlope = pSlope
	tick.MicroVWAPSlope = vSlope
	tick.MicroVolumeSlope = volSlope

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

// GetMicroSlopes instantly returns the current trajectory of the 60-second window
func (b *TokenRollingBuffer) GetMicroSlopes() (priceSlope, vwapSlope, volSlope float64) {
	return b.PriceReg.Slope(), b.VWAPReg.Slope(), b.VolReg.Slope()
}
