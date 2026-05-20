// internal/service/pipeline/enrichment.go
package pipeline

import (
	"gidh-backend/internal/service/order"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

// --- 1. Rolling Window Data Structures ---

type EnrichmentTick struct {
	Timestamp time.Time
	Volume    float64
	Price     float64
	VWAP      float64
	X         float64 // Normalized time (seconds since midnight)
}

// Track state machine status for the Golden Rule per stock
type ThresholdState struct {
	IsTesting            bool
	TimeCrossedThreshold time.Time
}

type TokenRollingBuffer struct {
	Ticks    []EnrichmentTick
	PriceReg RollingRegression
	VWAPReg  RollingRegression
	VolReg   RollingRegression

	// Golden Rule State Management
	StateMu        sync.Mutex
	ThresholdState ThresholdState
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks: make([]EnrichmentTick, 0, 2000), // Larger starting allocation for 5 mins of ticks
	}
}

// Push now accepts a configurable duration (pass 300 * time.Second)
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

	// Evict older than duration (e.g., 300 seconds)
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

const (
	ContinuousWindowDuration = 300 * time.Second // 5 Minutes Continuous Window
	SlopeThreshold           = 0.75              // Threshold for your linear regression slope change
	GoldenRuleHoldSeconds    = 5.0               // Must hold past threshold for 5 seconds
)

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
	ts := tick.Raw.Timestamp.In(s.loc) // Strictly IST

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

	// 2. Maintain Continuous 300-Second Sliding Window (UPGRADED)
	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}

	// Push with the updated 300s window limit
	buf.Push(ts, price, tick.Raw.AverageTradedPrice, vol, ContinuousWindowDuration)

	// Get live rolling stats
	liveVolume, liveTickCount := buf.GetStats()

	// 3. Compute Standard Relative Volume (RVol)
	var rVol float64
	if adv, ok := s.advMap[token]; ok && adv > 0 {
		expectedVolPerMin := adv / 375.0
		if expectedVolPerMin > 0 {
			rVol = liveVolume / expectedVolPerMin
		}
	}
	tick.RelativeVolume = rVol

	// 4. Calculate Minute Index & DNA Z-Scores
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555

	var volZ, tcZ float64
	if tokenDna, exists := s.dnaMap[token]; exists {
		if baseline, ok := tokenDna[minuteIndex]; ok {
			if baseline.VolumeStd > 0 {
				volZ = (liveVolume - baseline.VolumeMean) / baseline.VolumeStd
			}
			if baseline.TickCountStd > 0 {
				tcZ = (float64(liveTickCount) - baseline.TickCountMean) / baseline.TickCountStd
			}
		}
	}

	tick.VolumeZ = volZ
	tick.TickCountZ = tcZ

	// 5. GET CONTINUOUS 5-MINUTE LINEAR REGRESSION SLOPES
	pSlope, vSlope, volSlope := buf.GetMicroSlopes()
	tick.MicroPriceSlope = pSlope
	tick.MicroVWAPSlope = vSlope
	tick.MicroVolumeSlope = volSlope

	// 6. 🛡️ THE GOLDEN RULE STATE MACHINE (Per-Stock Execution Engine)
	buf.StateMu.Lock()

	// We check if the absolute slope value beats the threshold
	if math.Abs(pSlope) >= SlopeThreshold {
		if !buf.ThresholdState.IsTesting {
			// Cross-event verified: initialization entry point
			buf.ThresholdState.IsTesting = true
			buf.ThresholdState.TimeCrossedThreshold = ts
		} else {
			// Already printing past threshold. Evaluate persistence duration.
			timeHeld := ts.Sub(buf.ThresholdState.TimeCrossedThreshold).Seconds()
			if timeHeld >= GoldenRuleHoldSeconds {

				// ⚡ CORE TRADING ACTION TRIGGER EVENT ⚡
				// Anomaly confirmed, safe from micro wiggles!
				s.triggerGoldenRuleAlert(tick, pSlope, timeHeld)

				// Reset threshold timer state to avoid printing endless notifications
				buf.ThresholdState.IsTesting = false
			}
		}
	} else {
		// Slope failed to sustain direction. Wipe out state to prevent false execution flags.
		buf.ThresholdState.IsTesting = false
	}
	buf.StateMu.Unlock()

	return nil
}

func (s *EnrichmentStage) triggerGoldenRuleAlert(tick *models.EnrichedTick, confirmedSlope float64, duration float64) {
	// Add your execution code or custom signal transmission pipeline logic here
	println("🔥 [GOLDEN RULE CONFIRMED] Stock:", tick.Raw.StockName,
		"| Slope:", confirmedSlope,
		"| Held for:", duration, "seconds! Executing entry parameters near Structural Levels.")
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
