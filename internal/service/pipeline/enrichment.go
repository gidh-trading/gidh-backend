package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type InstrumentContext struct {
	LastVolume          int64
	LastPrice           float64
	Buffer              *TokenRollingBuffer
	DNA                 map[int]models.TimeBucketDNA
	IntervalPercentiles map[string]models.PercentileThresholds // 🔥 Baseline percentiles mapping for timeframe shapes
	Profile             *models.InstrumentProfile
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
			Buffer:              NewTokenRollingBuffer(),
			DNA:                 fastDnaMap,
			IntervalPercentiles: dna.IntervalPercentiles, // 🔥 Ingest multi-timeframe baseline shapes into context cache
			LastVolume:          0,
			LastPrice:           0.0,
			Profile:             profiles[token],
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
			Buffer:              NewTokenRollingBuffer(),
			DNA:                 make(map[int]models.TimeBucketDNA),
			IntervalPercentiles: make(map[string]models.PercentileThresholds),
			LastVolume:          0,
			LastPrice:           price,
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

	// Push current trade details onto the sliding 60-second matrix
	ctx.Buffer.Push(ts, price, float64(delta))
	liveVolume, liveTickCount, liveDisplacement := ctx.Buffer.GetProductionMetrics()

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	volRank := 4
	tickRank := 4
	priceRank := 4
	rangeRank := 4

	// Evaluate participation ranks via circadian baseline time frames
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

	// 🔥 FIXED: Dynamic Baseline-Driven Price Rank Normalization using 1m interval percentiles
	if baseline1m, has1mBaseline := ctx.IntervalPercentiles["1m"]; has1mBaseline {
		// Calculate structural body move magnitude
		absDisplacement := math.Abs(liveDisplacement)

		// Calculate total peak-to-trough boundary range
		_, _, rHigh, rLow, _ := ctx.Buffer.GetProductionStructure()
		liveRollingRange := rHigh - rLow

		// Track 1: Body Displacement Ranking (Compares against price_pXX)
		switch {
		case absDisplacement >= baseline1m.PriceP97:
			priceRank = 7
		case absDisplacement >= baseline1m.PriceP90:
			priceRank = 6
		case absDisplacement >= baseline1m.PriceP75:
			priceRank = 5
		case absDisplacement >= baseline1m.PriceP50:
			priceRank = 4
		case absDisplacement >= baseline1m.PriceP25:
			priceRank = 3
		case absDisplacement >= baseline1m.PriceP10:
			priceRank = 2
		default:
			priceRank = 1
		}

		// Track 2: Total Volatility Boundary Ranking (Compares against range_pXX)
		switch {
		case liveRollingRange >= baseline1m.RangeP97:
			rangeRank = 7
		case liveRollingRange >= baseline1m.RangeP90:
			rangeRank = 6
		case liveRollingRange >= baseline1m.RangeP75:
			rangeRank = 5
		case liveRollingRange >= baseline1m.RangeP50:
			rangeRank = 4
		case liveRollingRange >= baseline1m.RangeP25:
			rangeRank = 3
		case liveRollingRange >= baseline1m.RangeP10:
			rangeRank = 2
		default:
			rangeRank = 1
		}
	}

	tick.Enrichment = models.SimplifiedEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
		TickRank:    tickRank,
		PriceRank:   priceRank,
		RangeRank:   rangeRank,
	}

	return nil
}
