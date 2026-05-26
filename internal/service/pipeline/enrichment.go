package pipeline

import (
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
	liveVolume, _, _ := ctx.Buffer.GetProductionMetrics()

	// 4. Build baseline index markers
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	// 5. Evaluate non-gaussian percentile ranks
	volRank := 4 // Default balanced baseline coordinate

	if baseline, ok := ctx.DNA[minuteIndex]; ok {
		switch {
		case liveVolume >= baseline.VolumeP97:
			volRank = 7 // Extreme Burst
		case liveVolume >= baseline.VolumeP90:
			volRank = 6 // Elevated Activity
		case liveVolume >= baseline.VolumeP75:
			volRank = 5 // Active Participation
		case liveVolume >= baseline.VolumeP50:
			volRank = 4 // Normal baseline
		case liveVolume >= baseline.VolumeP25:
			volRank = 3 // Below Normal
		case liveVolume >= baseline.VolumeP10:
			volRank = 2 // Weak
		default:
			volRank = 1 // Drought Minimal
		}
	}

	tick.Enrichment = models.SimplifiedEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
	}

	return nil
}
