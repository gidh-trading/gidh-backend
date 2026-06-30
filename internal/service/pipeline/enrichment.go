package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type InstrumentContext struct {
	mu                  sync.Mutex // 🟢 Granular per-token lock to decouple workers
	LastVolume          int64
	LastPrice           float64
	Buffer              *TokenRollingBuffer
	DNA                 map[int]models.TimeBucketDNA
	IntervalPercentiles map[string]models.PercentileThresholds
	Profile             *models.InstrumentProfile
}

type EnrichmentStage struct {
	instruments     map[uint32]*InstrumentContext
	positionManager order.PositionManager
	loc             *time.Location
	mu              sync.RWMutex // 🟢 Changed to RWMutex for high-speed concurrent lookups
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
			IntervalPercentiles: dna.IntervalPercentiles,
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

func (es *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(es.loc)

	// 1. Fast concurrent read-lock lookup
	es.mu.RLock()
	ctx, exists := es.instruments[token]
	es.mu.RUnlock()

	// 2. Safe lazy initialization if a completely unique unmapped token arrives
	if !exists {
		es.mu.Lock()
		// Double-check after acquiring write lock
		ctx, exists = es.instruments[token]
		if !exists {
			ctx = &InstrumentContext{
				Buffer:              NewTokenRollingBuffer(),
				DNA:                 make(map[int]models.TimeBucketDNA),
				IntervalPercentiles: make(map[string]models.PercentileThresholds),
				LastVolume:          0,
				LastPrice:           price,
			}
			es.instruments[token] = ctx
		}
		es.mu.Unlock()
	}

	// 3. ⚡ LOCK ONLY THIS INSTRUMENT: Other tokens proceed completely unhindered
	ctx.mu.Lock()
	defer ctx.mu.Unlock()

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

	if price != ctx.LastPrice && es.positionManager != nil {
		es.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}

	ctx.LastPrice = price

	ctx.Buffer.Push(ts, price, float64(delta))
	liveVolume, liveTickCount, liveDisplacement := ctx.Buffer.GetProductionMetrics()

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	volRank := 4
	tickRank := 4
	priceRank := 4
	rangeRank := 4

	if baseline, ok := ctx.DNA[minuteIndex]; ok {
		var absoluteVolumeVelocityFloor float64 = 0.0
		var globalPaceFloor float64 = 0.0

		if ctx.Profile != nil && ctx.Profile.ADV30d > 0 {
			averageVolPerMinute := float64(ctx.Profile.ADV30d) / 375.0
			globalPaceFloor = averageVolPerMinute * 0.85

			currentHourMinute := (ts.Hour() * 100) + ts.Minute()
			var sessionMultiplier float64 = 1.0

			switch {
			case currentHourMinute >= 915 && currentHourMinute < 1030:
				sessionMultiplier = 0.97
			case currentHourMinute >= 1030 && currentHourMinute < 1415:
				sessionMultiplier = 0.70
			case currentHourMinute >= 1415 && currentHourMinute <= 1530:
				sessionMultiplier = 0.97
			}

			absoluteVolumeVelocityFloor = averageVolPerMinute * sessionMultiplier * 0.95
		}

		switch {
		case liveVolume >= baseline.VolumeP99: // 🟢 Added P99 threshold check
			if liveVolume >= absoluteVolumeVelocityFloor && liveVolume >= globalPaceFloor {
				volRank = 8
			} else {
				volRank = 5
			}
		case liveVolume >= baseline.VolumeP97:
			if liveVolume >= absoluteVolumeVelocityFloor && liveVolume >= globalPaceFloor {
				volRank = 7
			} else {
				volRank = 5
			}
		case liveVolume >= baseline.VolumeP90:
			if liveVolume >= absoluteVolumeVelocityFloor && liveVolume >= globalPaceFloor {
				volRank = 6
			} else {
				volRank = 5
			}
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

		// --- 2. Tick Count Rank Check with P99 addition ---
		floatTickCount := float64(liveTickCount)
		switch {
		case floatTickCount >= baseline.TickCountP99:
			tickRank = 8
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

	_, _, rHigh, rLow, _ := ctx.Buffer.GetProductionStructure()
	liveRollingRange := rHigh - rLow

	if baseline1m, has1mBaseline := ctx.IntervalPercentiles["1m"]; has1mBaseline {
		absDisplacement := math.Abs(liveDisplacement)

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

	wOpen := price - liveDisplacement
	wClose := price
	wHigh := rHigh
	wLow := rLow

	trueDirection := es.calculateDirection(wOpen, wHigh, wLow, wClose, volRank, priceRank)

	tick.Enrichment = models.TickEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
		TickRank:    tickRank,
		PriceRank:   priceRank,
		RangeRank:   rangeRank,
		Direction:   trueDirection,
	}

	return nil
}

func (es *EnrichmentStage) GetInstrumentProfile(token uint32) (*models.InstrumentProfile, bool) {
	es.mu.RLock()
	defer es.mu.RUnlock()
	ctx, exists := es.instruments[token]
	if !exists || ctx == nil {
		return nil, false
	}
	return ctx.Profile, true
}

func (es *EnrichmentStage) calculateDirection(wOpen, wHigh, wLow, wClose float64, volRank, priceRank int) models.DirectionState {
	windowRange := wHigh - wLow
	if windowRange <= 0 {
		return models.DirSideways
	}
	positionRatio := (wClose - wLow) / windowRange
	isHigherThanOpen := wClose > wOpen
	isLowerThanOpen := wClose < wOpen

	if volRank >= 6 && priceRank <= 4 {
		if positionRatio >= 0.50 {
			return models.DirBullishAbsorption
		}
		if positionRatio < 0.50 {
			return models.DirBearishAbsorption
		}
	}

	switch {
	case positionRatio >= 0.85 && isHigherThanOpen:
		return models.DirStrongBullish
	case positionRatio > 0.55 && isHigherThanOpen:
		return models.DirBullish
	case positionRatio <= 0.15 && isLowerThanOpen:
		return models.DirStrongBearish
	case positionRatio < 0.45 && isLowerThanOpen:
		return models.DirBearish
	default:
		return models.DirSideways
	}
}
