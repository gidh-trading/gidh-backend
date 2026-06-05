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

func (es *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	es.mu.Lock()
	defer es.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(es.loc)

	ctx, exists := es.instruments[token]
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

	// Extract total peak-to-trough boundary range variables for ranking
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

	// ========================================================================
	// 🔥 CALCULATE TRUE MICROSTRUCTURAL DIRECTION (ADDED HERE)
	// ========================================================================
	// Since liveDisplacement = wClose - wOpen, we know that wOpen = wClose - liveDisplacement
	wOpen := price - liveDisplacement
	wClose := price
	wHigh := rHigh
	wLow := rLow

	trueDirection := es.calculateDirection(wOpen, wHigh, wLow, wClose, volRank, priceRank)

	// Ingest directional classification seamlessly straight onto the structured output sub-object
	tick.Enrichment = models.TickEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
		TickRank:    tickRank,
		PriceRank:   priceRank,
		RangeRank:   rangeRank,
		Direction:   trueDirection, // Matches your updated TickEnrichment structural properties layout
	}

	return nil
}

func (es *EnrichmentStage) GetInstrumentProfile(token uint32) (*models.InstrumentProfile, bool) {
	es.mu.Lock()
	defer es.mu.Unlock()
	ctx, exists := es.instruments[token]
	if !exists || ctx == nil {
		return nil, false
	}
	return ctx.Profile, true
}

// calculateDirection applies pure mathematical range positioning and volume blockage checks to determine state
func (es *EnrichmentStage) calculateDirection(wOpen, wHigh, wLow, wClose float64, volRank, priceRank int) models.DirectionState {
	windowRange := wHigh - wLow

	// Safety check for fresh tokens or perfectly illiquid static markets
	if windowRange <= 0 {
		return models.DirSideways
	}

	// Calculate where the current close price sits inside the 1-minute range as a percentage (0.0 to 1.0)
	positionRatio := (wClose - wLow) / windowRange

	// Evaluate displacement relative to the window open
	isHigherThanOpen := wClose > wOpen
	isLowerThanOpen := wClose < wOpen

	// ========================================================================
	// 🔥 MICROSTRUCTURAL RE-CLASSIFICATION: DETECT MICRO-ABSORPTION
	// ========================================================================
	// If participation volume is at abnormal institutional intensity (P90+)
	// but price velocity/displacement remains completely capped (PriceRank <= 4)
	if volRank >= 6 && priceRank <= 4 {
		// High close position ratio within the rolling matrix = Passive limit buying absorption wall
		if positionRatio >= 0.50 {
			return models.DirBullishAbsorption
		}
		// Low close position ratio within the rolling matrix = Passive limit selling absorption ceiling
		if positionRatio < 0.50 {
			return models.DirBearishAbsorption
		}
	}

	switch {
	// Top 15% of the range + upward displacement = Severe Buying Urgency
	case positionRatio >= 0.85 && isHigherThanOpen:
		return models.DirStrongBullish

	// Upper half of the range + upward displacement = Steady Accumulation
	case positionRatio > 0.55 && isHigherThanOpen:
		return models.DirBullish

	// Bottom 15% of the range + downward displacement = Severe Liquidating Pressure
	case positionRatio <= 0.15 && isLowerThanOpen:
		return models.DirStrongBearish

	// Lower half of the range + downward displacement = Steady Distribution
	case positionRatio < 0.45 && isLowerThanOpen:
		return models.DirBearish

	// Normal rotational low-volume balance churn zone
	default:
		return models.DirSideways
	}
}
