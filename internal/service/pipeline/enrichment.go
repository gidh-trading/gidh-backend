package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type InstrumentContext struct {
	LastVolume    int64
	LastPrice     float64
	CurrentBarMin int
	BarOpenPrice  float64
	Buffer        *TokenRollingBuffer
	DNA           map[int]models.TimeBucketDNA
	Profile       *models.InstrumentProfile // Injected profile data
}

type EnrichmentStage struct {
	instruments     map[uint32]*InstrumentContext
	positionManager order.PositionManager
	loc             *time.Location
	mu              sync.Mutex
}

func NewEnrichmentStage(pm order.PositionManager, rawDnaMap map[uint32]*models.MarketDNA, profiles map[uint32]*models.InstrumentProfile) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	instruments := make(map[uint32]*InstrumentContext)

	for token, dna := range rawDnaMap {
		fastDnaMap := make(map[int]models.TimeBucketDNA)
		for _, bucket := range dna.TimeBuckets {
			fastDnaMap[bucket.MinuteIndex] = bucket
		}

		instruments[token] = &InstrumentContext{
			Buffer:        NewTokenRollingBuffer(),
			DNA:           fastDnaMap,
			LastVolume:    0,
			LastPrice:     0.0,
			CurrentBarMin: -1,
			BarOpenPrice:  0.0,
			Profile:       profiles[token], // Store reference to ATR configurations
		}
	}

	return &EnrichmentStage{
		instruments:     instruments,
		positionManager: pm,
		loc:             loc,
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(s.loc)

	ctx, exists := s.instruments[token]
	if !exists {
		ctx = &InstrumentContext{
			Buffer:        NewTokenRollingBuffer(),
			DNA:           make(map[int]models.TimeBucketDNA),
			LastVolume:    0,
			LastPrice:     price,
			CurrentBarMin: ts.Minute(),
			BarOpenPrice:  price,
		}
		s.instruments[token] = ctx
	}

	curr := tick.Raw.CumulativeVolume
	prev := ctx.LastVolume
	var delta int64
	if prev == 0 || curr < prev {
		delta = tick.Raw.LastTradedQuantity
	} else {
		delta = curr - prev
	}
	ctx.LastVolume = curr
	tick.TickVolume = delta

	if tick.TickVolume == 0 && price == ctx.LastPrice {
		return nil
	}

	if price != ctx.LastPrice && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	ctx.LastPrice = price

	ctx.Buffer.Push(ts, price, float64(delta))
	liveVolume, liveTickCount, _ := ctx.Buffer.GetProductionMetrics()

	currentClockMin := ts.Minute()
	if ctx.CurrentBarMin == -1 || currentClockMin != ctx.CurrentBarMin {
		ctx.CurrentBarMin = currentClockMin
		ctx.BarOpenPrice = price
	}

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	volRank := 4
	tickRank := 4
	priceRank := 4

	// 1. EVALUATE PARTICIPATION (Safe to use circadian time baselines)
	if baseline, ok := ctx.DNA[minuteIndex]; ok {
		switch {
		case liveVolume >= baseline.VolumeP97:
			volRank = 7
		case liveVolume >= baseline.VolumeP90:
			volRank = 6
		case liveVolume >= baseline.VolumeP75:
			volRank = 5
		case liveVolume >= baseline.VolumeP50:
			volRank = 4
		case liveVolume >= baseline.VolumeP25:
			volRank = 3
		case liveVolume >= baseline.VolumeP10:
			volRank = 2
		default:
			volRank = 1
		}

		floatTickCount := float64(liveTickCount)
		switch {
		case floatTickCount >= baseline.TickCountP97:
			tickRank = 7
		case floatTickCount >= baseline.TickCountP90:
			tickRank = 6
		case floatTickCount >= baseline.TickCountP75:
			tickRank = 5
		case floatTickCount >= baseline.TickCountP50:
			tickRank = 4
		case floatTickCount >= baseline.TickCountP25:
			tickRank = 3
		case floatTickCount >= baseline.TickCountP10:
			tickRank = 2
		default:
			tickRank = 1
		}
	}

	// 2. 🔥 STATISTICALLY RELIABLE ATR NORMALIZATION
	absCandleRange := math.Abs(price - ctx.BarOpenPrice)

	if ctx.Profile != nil && ctx.Profile.ATR14 > 0 {
		// Measures the raw percentage coefficient of the full 14-day daily range traversed in 60s
		volatilityFactor := absCandleRange / float64(ctx.Profile.ATR14)

		switch {
		case volatilityFactor >= 0.050:
			priceRank = 7 // True Extreme Breakout Velocity (Saturated Magenta)
		case volatilityFactor >= 0.030:
			priceRank = 6 // Significant Velocity Expansion
		case volatilityFactor >= 0.015:
			priceRank = 5 // Active Expansion
		case volatilityFactor >= 0.005:
			priceRank = 4 // Normal Structural Mean progression
		case volatilityFactor >= 0.002:
			priceRank = 3 // Suppressed Expansion / High-Volume Anomaly Absorption
		case volatilityFactor >= 0.001:
			priceRank = 2 // Low Volatility Churn Space
		default:
			priceRank = 1 // Absolute Range Squeeze / Deadlock State
		}
	}

	tick.Enrichment = models.SimplifiedEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
		TickRank:    tickRank,
		PriceRank:   priceRank,
	}

	return nil
}
