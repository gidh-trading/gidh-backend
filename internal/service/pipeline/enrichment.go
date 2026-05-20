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
	X         float64
	RawTicks  int
}

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

	// --- 📈 SMOOTHING & SAMPLING VARIABLES (NEW) ---
	LastSampleTime     time.Time // Tracks when we last added a node to the regression matrix
	SmoothedPriceSlope float64   // Moving average of the price slope
	SmoothedVWAPSlope  float64   // Moving average of the VWAP slope
	SmoothedVolSlope   float64   // Moving average of the Volume slope
	AccumulatedVolume  float64   // Buffers volume between sample windows
	AccumulatedTicks   int
}

func NewTokenRollingBuffer() *TokenRollingBuffer {
	return &TokenRollingBuffer{
		Ticks: make([]EnrichmentTick, 0, 2000),
	}
}

// Push now samples data points to prevent high-frequency tick noise from warping regressions
func (b *TokenRollingBuffer) Push(ts time.Time, price, vwap, vol float64, duration time.Duration, minSampleDelta time.Duration) bool {
	// Buffer volume and ticks continually
	b.AccumulatedVolume += vol
	b.AccumulatedTicks++ // 👈 ADD THIS

	if !b.LastSampleTime.IsZero() && ts.Sub(b.LastSampleTime) < minSampleDelta {
		return false
	}

	b.LastSampleTime = ts
	sampledVol := b.AccumulatedVolume
	sampledTicks := b.AccumulatedTicks // 👈 ADD THIS

	b.AccumulatedVolume = 0
	b.AccumulatedTicks = 0 // 👈 ADD THIS

	x := float64(ts.Hour()*3600 + ts.Minute()*60 + ts.Second())

	tick := EnrichmentTick{
		Timestamp: ts,
		Volume:    sampledVol,
		Price:     price,
		VWAP:      vwap,
		X:         x,
		RawTicks:  sampledTicks,
	}

	b.Ticks = append(b.Ticks, tick)

	// O(1) Add to regression running totals
	b.PriceReg.Add(x, price)
	b.VWAPReg.Add(x, vwap)
	b.VolReg.Add(x, sampledVol)

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
	return true // Regression statistics were updated
}

func (b *TokenRollingBuffer) GetStats() (float64, int) {
	var totalVol float64
	var totalTicks int
	for _, t := range b.Ticks {
		totalVol += t.Volume
		totalTicks += t.RawTicks
	}
	totalVol += b.AccumulatedVolume
	totalTicks += b.AccumulatedTicks
	return totalVol, totalTicks
}

// --- 2. Enrichment Stage ---

const (
	ContinuousWindowDuration = 60 * time.Second

	// 🛡️ IDEA 3: DUAL-BAND HYSTERESIS THRESHOLDS
	ActivationThreshold   = 0.75 // Strong momentum required to alert
	DeactivationThreshold = 0.45 // Lower threshold floor before dropping testing state

	GoldenRuleHoldSeconds = 5.0 // Must hold past threshold for 5 seconds

	// 📊 IDEA 1 & 2: FILTER PARAMETERS
	SlopeAlpha            = 0.15            // EMA factor. Lower = smoother, higher = more responsive.
	DefaultSampleInterval = 1 * time.Second // Time slice size for regression nodes
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
	ts := tick.Raw.Timestamp.In(s.loc)

	tick.TickVolume = s.calculateTickVolume(token, tick)
	vol := float64(tick.TickVolume)

	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	priceChanged := tick.Raw.LastPrice != s.lastPriceMap[token]
	if priceChanged && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	s.lastPriceMap[token] = price

	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}

	// Calculate Minute Index to interface with your structural DNA buckets
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555

	// 🧠 INTERFACING WITH DNA BUCKETS:
	// Optimize resampling delta based on historical volatility activity definitions!
	sampleInterval := DefaultSampleInterval
	if tokenDna, exists := s.dnaMap[token]; exists {
		if baseline, ok := tokenDna[minuteIndex]; ok {
			// If historical frequency activity is high, sample faster to capture institutional shifts
			if baseline.TickCountMean > 500 {
				sampleInterval = 500 * time.Millisecond
			} else if baseline.TickCountMean < 50 {
				sampleInterval = 2 * time.Second
			}
		}
	}

	// Push tick and check if a new regression coordinate slice was generated
	statsUpdated := buf.Push(ts, price, tick.Raw.AverageTradedPrice, vol, ContinuousWindowDuration, sampleInterval)

	// 📈 IDEA 1: APPLY EMA SMOOTHING MATRIX OVER THE RAW TRAJECTORY
	if statsUpdated {
		rawPSlope, rawVSlope, rawVolSlope := buf.PriceReg.Slope(), buf.VWAPReg.Slope(), buf.VolReg.Slope()

		// Handle bootstrap state initialization
		if buf.LastSampleTime.Equal(ts) && len(buf.Ticks) <= 2 {
			buf.SmoothedPriceSlope = rawPSlope
			buf.SmoothedVWAPSlope = rawVSlope
			buf.SmoothedVolSlope = rawVolSlope
		} else {
			// Apply Exponential Moving Average math
			buf.SmoothedPriceSlope = (SlopeAlpha * rawPSlope) + ((1.0 - SlopeAlpha) * buf.SmoothedPriceSlope)
			buf.SmoothedVWAPSlope = (SlopeAlpha * rawVSlope) + ((1.0 - SlopeAlpha) * buf.SmoothedVWAPSlope)
			buf.SmoothedVolSlope = (SlopeAlpha * rawVolSlope) + ((1.0 - SlopeAlpha) * buf.SmoothedVolSlope)
		}
	}

	// Inject the clean, non-wiggling slope properties downstream
	tick.MicroPriceSlope = buf.SmoothedPriceSlope
	tick.MicroVWAPSlope = buf.SmoothedVWAPSlope
	tick.MicroVolumeSlope = buf.SmoothedVolSlope

	// Recompute standard Z-scores and RVol stats...
	liveVolume, liveTickCount := buf.GetStats()
	var volZ, tcZ float64
	var rollingVolMean float64

	if tokenDna, exists := s.dnaMap[token]; exists {
		if currBaseline, ok := tokenDna[minuteIndex]; ok {
			sec := float64(ts.Second())
			prevBaseline := currBaseline

			// Get previous minute baseline if available
			if minuteIndex > 0 {
				if pb, ok := tokenDna[minuteIndex-1]; ok {
					prevBaseline = pb
				}
			}

			// Calculate weights based on how far into the current minute we are
			weightCurr := sec / 60.0
			weightPrev := (60.0 - sec) / 60.0

			// Interpolate means and standard deviations
			rollingVolMean = (currBaseline.VolumeMean * weightCurr) + (prevBaseline.VolumeMean * weightPrev)
			rollingVolStd := (currBaseline.VolumeStd * weightCurr) + (prevBaseline.VolumeStd * weightPrev)
			rollingTcMean := (currBaseline.TickCountMean * weightCurr) + (prevBaseline.TickCountMean * weightPrev)
			rollingTcStd := (currBaseline.TickCountStd * weightCurr) + (prevBaseline.TickCountStd * weightPrev)

			// Calculate mathematically sound Z-Scores
			if rollingVolStd > 0 {
				volZ = (liveVolume - rollingVolMean) / rollingVolStd
			}
			if rollingTcStd > 0 {
				tcZ = (float64(liveTickCount) - rollingTcMean) / rollingTcStd
			}
		}
	}

	var rVol float64
	if rollingVolMean > 0 {
		rVol = liveVolume / rollingVolMean
	} else if adv, ok := s.advMap[token]; ok && adv > 0 {
		// Fallback to flat ADV only if DNA is missing
		if expectedVolPerMin := adv / 375.0; expectedVolPerMin > 0 {
			rVol = liveVolume / expectedVolPerMin
		}
	}

	tick.RelativeVolume = rVol
	tick.VolumeZ = volZ
	tick.TickCountZ = tcZ

	// 🛡️ IDEA 3: THE DUAL-BAND HYSTERESIS STATE MACHINE
	buf.StateMu.Lock()
	absSlope := math.Abs(tick.MicroPriceSlope)

	if !buf.ThresholdState.IsTesting {
		// Entry to entry confirmation mode requires breaching the strict upper ceiling
		if absSlope >= ActivationThreshold {
			buf.ThresholdState.IsTesting = true
			buf.ThresholdState.TimeCrossedThreshold = ts
		}
	} else {
		// Inside execution loops, do not wipe state on minor pullbacks.
		// Only break out if momentum collapses completely past the deactivation floor.
		if absSlope >= DeactivationThreshold {
			timeHeld := ts.Sub(buf.ThresholdState.TimeCrossedThreshold).Seconds()
			if timeHeld >= GoldenRuleHoldSeconds {
				s.triggerGoldenRuleAlert(tick, tick.MicroPriceSlope, timeHeld)
				buf.ThresholdState.IsTesting = false
			}
		} else {
			// Pullback violated structural trend floor; drop validation parameters
			buf.ThresholdState.IsTesting = false
		}
	}
	buf.StateMu.Unlock()

	return nil
}

func (s *EnrichmentStage) triggerGoldenRuleAlert(tick *models.EnrichedTick, confirmedSlope float64, duration float64) {
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
