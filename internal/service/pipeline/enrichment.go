package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

// classifyPercentile maps the raw value against the DNA baseline percentiles
func classifyPercentile(value, p05, p10, p25, p50, p75, p90, p97 float64) string {
	switch {
	case value >= p97:
		return "P97" // burst/extreme
	case value >= p90:
		return "P90" // elevated
	case value >= p75:
		return "P75" // active
	case value >= p50:
		return "P50" // baseline
	case value >= p25:
		return "P25" // below normal
	case value >= p10:
		return "P10" // weak
	case value >= p05:
		return "P05" // drought
	default:
		return "DROUGHT_EXTREME" // Anything below P05 falls entirely below the grid floor
	}
}

// InstrumentContext groups all rolling state and historical data per instrument
type InstrumentContext struct {
	LastVolume int64
	LastPrice  float64
	Buffer     *TokenRollingBuffer
	DNA        map[int]models.TimeBucketDNA
}

type EnrichmentStage struct {
	instruments     map[uint32]*InstrumentContext
	positionManager order.PositionManager
	loc             *time.Location
	mu              sync.Mutex
}

func NewEnrichmentStage(pm order.PositionManager, rawDnaMap map[uint32]*models.MarketDNA) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	instruments := make(map[uint32]*InstrumentContext)

	// Pre-build the unified context for every active instrument
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

	// 1. Grab unified state or initialize it if it's a new token
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

	// 2. Calculate volume delta using the context
	tick.TickVolume = s.calculateTickVolume(ctx, tick)
	volDelta := float64(tick.TickVolume)

	// Ignore zero-volume ticks that don't move the price
	if tick.TickVolume == 0 && price == ctx.LastPrice {
		return nil
	}

	// 3. Trigger Position Manager logic ONLY on price changes
	if price != ctx.LastPrice && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	ctx.LastPrice = price

	// 4. Push to rolling buffer
	ctx.Buffer.Push(ts, price, volDelta)

	// 5. Fetch exactly the metrics requested (Volume, TickCount, Displacement)
	liveVolume, liveTickCount, liveDisplacement := ctx.Buffer.GetProductionMetrics()

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex
	tick.EnrichedAt = time.Now().UnixMilli()

	// 6. Populate the clean LiveTelemetry struct
	tick.Telemetry = models.LiveTelemetry{
		MinuteIndex:      minuteIndex,
		TickCount:        liveTickCount,
		LiveVolume:       liveVolume,
		LiveDisplacement: liveDisplacement,
	}

	// Default fallbacks if no DNA exists
	volPct, pricePct, tickPct := "NORMAL", "NORMAL", "NORMAL"

	// 7. DNA Baseline Percentile Matching
	if currBaseline, ok := ctx.DNA[minuteIndex]; ok {
		tick.DNASampleCount = currBaseline.SampleCount

		sec := float64(ts.Second())
		prevBaseline := currBaseline
		if minuteIndex > 0 {
			if pb, ok := ctx.DNA[minuteIndex-1]; ok {
				prevBaseline = pb
			}
		}

		weightCurr := sec / 60.0
		weightPrev := (60.0 - sec) / 60.0

		// A. Volume Percentile
		v05 := (currBaseline.VolumeP05 * weightCurr) + (prevBaseline.VolumeP05 * weightPrev)
		v10 := (currBaseline.VolumeP10 * weightCurr) + (prevBaseline.VolumeP10 * weightPrev)
		v25 := (currBaseline.VolumeP25 * weightCurr) + (prevBaseline.VolumeP25 * weightPrev)
		v50 := (currBaseline.VolumeP50 * weightCurr) + (prevBaseline.VolumeP50 * weightPrev)
		v75 := (currBaseline.VolumeP75 * weightCurr) + (prevBaseline.VolumeP75 * weightPrev)
		v90 := (currBaseline.VolumeP90 * weightCurr) + (prevBaseline.VolumeP90 * weightPrev)
		v97 := (currBaseline.VolumeP97 * weightCurr) + (prevBaseline.VolumeP97 * weightPrev)
		volPct = classifyPercentile(liveVolume, v05, v10, v25, v50, v75, v90, v97)

		// B. Price Displacement Percentile
		p05 := (currBaseline.PriceP05 * weightCurr) + (prevBaseline.PriceP05 * weightPrev)
		p10 := (currBaseline.PriceP10 * weightCurr) + (prevBaseline.PriceP10 * weightPrev)
		p25 := (currBaseline.PriceP25 * weightCurr) + (prevBaseline.PriceP25 * weightPrev)
		p50 := (currBaseline.PriceP50 * weightCurr) + (prevBaseline.PriceP50 * weightPrev)
		p75 := (currBaseline.PriceP75 * weightCurr) + (prevBaseline.PriceP75 * weightPrev)
		p90 := (currBaseline.PriceP90 * weightCurr) + (prevBaseline.PriceP90 * weightPrev)
		p97 := (currBaseline.PriceP97 * weightCurr) + (prevBaseline.PriceP97 * weightPrev)
		pricePct = classifyPercentile(liveDisplacement, p05, p10, p25, p50, p75, p90, p97)

		// C. Tick Count Percentile
		tc05 := (currBaseline.TickCountP05 * weightCurr) + (prevBaseline.TickCountP05 * weightPrev)
		tc10 := (currBaseline.TickCountP10 * weightCurr) + (prevBaseline.TickCountP10 * weightPrev)
		tc25 := (currBaseline.TickCountP25 * weightCurr) + (prevBaseline.TickCountP25 * weightPrev)
		tc50 := (currBaseline.TickCountP50 * weightCurr) + (prevBaseline.TickCountP50 * weightPrev)
		tc75 := (currBaseline.TickCountP75 * weightCurr) + (prevBaseline.TickCountP75 * weightPrev)
		tc90 := (currBaseline.TickCountP90 * weightCurr) + (prevBaseline.TickCountP90 * weightPrev)
		tc97 := (currBaseline.TickCountP97 * weightCurr) + (prevBaseline.TickCountP97 * weightPrev)
		tickPct = classifyPercentile(float64(liveTickCount), tc05, tc10, tc25, tc50, tc75, tc90, tc97)
	}

	// 8. Populate Final State
	tick.Enrichment = models.EnrichmentState{
		MinuteIndex:      minuteIndex,
		VolumePercentile: volPct,
		PricePercentile:  pricePct,
		TickPercentile:   tickPct,
		Timestamp:        ts,
	}

	return nil
}

func (s *EnrichmentStage) calculateTickVolume(ctx *InstrumentContext, tick *models.EnrichedTick) int64 {
	curr := tick.Raw.CumulativeVolume
	prev := ctx.LastVolume
	var delta int64
	switch {
	case prev == 0:
		delta = tick.Raw.LastTradedQuantity
	case curr >= prev:
		delta = curr - prev
	default:
		delta = tick.Raw.LastTradedQuantity
	}
	ctx.LastVolume = curr
	return delta
}
