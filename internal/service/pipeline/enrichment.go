package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type InstrumentContext struct {
	LastVolume int64
	LastPrice  float64
	Buffer     *TokenRollingBuffer
	DNA        map[int]models.TimeBucketDNA
}

type EnrichmentStage struct {
	instruments     map[uint32]*InstrumentContext
	positionManager order.PositionManager // Restored for live/paper execution engines
	loc             *time.Location
	mu              sync.Mutex
}

// NewEnrichmentStage maps baseline definitions and injects the active position manager
func NewEnrichmentStage(pm order.PositionManager, rawDnaMap map[uint32]*models.MarketDNA) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	instruments := make(map[uint32]*InstrumentContext)

	for token, dna := range rawDnaMap {
		fastDnaMap := make(map[int]models.TimeBucketDNA)
		for _, bucket := range dna.TimeBuckets {
			fastDnaMap[bucket.MinuteIndex] = bucket
		}

		instruments[token] = &InstrumentContext{
			Buffer:     NewTokenRollingBuffer(),
			DNA:        fastDnaMap,
			LastVolume: 0,
			LastPrice:  0.0,
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
			Buffer:     NewTokenRollingBuffer(),
			DNA:        make(map[int]models.TimeBucketDNA),
			LastVolume: 0,
			LastPrice:  price,
		}
		s.instruments[token] = ctx
	}

	// 1. Compute direct standalone tick-by-tick volume delta
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

	// Drop silent ticks to save cycles
	if tick.TickVolume == 0 && price == ctx.LastPrice {
		return nil
	}

	// 2. CRITICAL RESTORATION: Route price updates directly to execution rules on change
	if price != ctx.LastPrice && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	ctx.LastPrice = price

	// 3. Track 60s continuous window metrics
	ctx.Buffer.Push(ts, price, float64(delta))
	liveVolume, liveTickCount, liveDisplacement := ctx.Buffer.GetProductionMetrics()

	// 4. Build baseline index markers
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	// 5. Evaluate non-gaussian percentile ranks
	volRank := 4
	tickRank := 4
	priceRank := 4

	if baseline, ok := ctx.DNA[minuteIndex]; ok {
		// Calculate Volume Rank
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

		// Calculate Tick Rank (Execution Churn Activity Intensity)
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

		// Calculate Price Velocity Rank (Spatial Vector Intensity)
		absDisp := math.Abs(liveDisplacement)
		switch {
		case absDisp >= baseline.PriceP97:
			priceRank = 7
		case absDisp >= baseline.PriceP90:
			priceRank = 6
		case absDisp >= baseline.PriceP75:
			priceRank = 5
		case absDisp >= baseline.PriceP50:
			priceRank = 4
		case absDisp >= baseline.PriceP25:
			priceRank = 3
		case absDisp >= baseline.PriceP10:
			priceRank = 2
		default:
			priceRank = 1
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
