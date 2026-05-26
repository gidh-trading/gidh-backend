package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

// InstrumentContext groups all rolling state and historical data per instrument
type InstrumentContext struct {
	LastVolume     int64
	LastPrice      float64
	Buffer         *TokenRollingBuffer
	DNA            map[int]models.TimeBucketDNA
	SessionCanvas  *models.SessionContext // Day-long continuous chronological timeline vector
	LastSavedIndex int                    // Deduplicates snapshot updates by checking minute rollovers
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
			SessionCanvas: &models.SessionContext{
				Timeline:       make([]models.SessionSnapshot, 0, 400), // Pre-allocate daily room
				MaxStoredSteps: 375,                                    // Total trading minutes in an NSE cash session
			},
			LastSavedIndex: -1,
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
			SessionCanvas: &models.SessionContext{
				Timeline:       make([]models.SessionSnapshot, 0, 400),
				MaxStoredSteps: 375,
			},
			LastSavedIndex: -1,
		}
		s.instruments[token] = ctx
	}

	tick.TickVolume = s.calculateTickVolume(ctx, tick)
	volDelta := float64(tick.TickVolume)

	if tick.TickVolume == 0 && price == ctx.LastPrice {
		return nil
	}

	if price != ctx.LastPrice && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	ctx.LastPrice = price

	ctx.Buffer.Push(ts, price, volDelta)

	liveVolume, liveTickCount, liveDisplacement := ctx.Buffer.GetProductionMetrics()

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex
	tick.EnrichedAt = time.Now().UnixMilli()

	tick.Telemetry = models.LiveTelemetry{
		MinuteIndex:      minuteIndex,
		TickCount:        liveTickCount,
		LiveVolume:       liveVolume,
		LiveDisplacement: liveDisplacement,
	}

	volPct, pricePct, tickPct := "NORMAL", "NORMAL", "NORMAL"

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

		v05 := (currBaseline.VolumeP05 * weightCurr) + (prevBaseline.VolumeP05 * weightPrev)
		v10 := (currBaseline.VolumeP10 * weightCurr) + (prevBaseline.VolumeP10 * weightPrev)
		v25 := (currBaseline.VolumeP25 * weightCurr) + (prevBaseline.VolumeP25 * weightPrev)
		v50 := (currBaseline.VolumeP50 * weightCurr) + (prevBaseline.VolumeP50 * weightPrev)
		v75 := (currBaseline.VolumeP75 * weightCurr) + (prevBaseline.VolumeP75 * weightPrev)
		v90 := (currBaseline.VolumeP90 * weightCurr) + (prevBaseline.VolumeP90 * weightPrev)
		v97 := (currBaseline.VolumeP97 * weightCurr) + (prevBaseline.VolumeP97 * weightPrev)
		volPct = classifyPercentile(liveVolume, v05, v10, v25, v50, v75, v90, v97)

		p05 := (currBaseline.PriceP05 * weightCurr) + (prevBaseline.PriceP05 * weightPrev)
		p10 := (currBaseline.PriceP10 * weightCurr) + (prevBaseline.PriceP10 * weightPrev)
		p25 := (currBaseline.PriceP25 * weightCurr) + (prevBaseline.PriceP25 * weightPrev)
		p50 := (currBaseline.PriceP50 * weightCurr) + (prevBaseline.PriceP50 * weightPrev)
		p75 := (currBaseline.PriceP75 * weightCurr) + (prevBaseline.PriceP75 * weightPrev)
		p90 := (currBaseline.PriceP90 * weightCurr) + (prevBaseline.PriceP90 * weightPrev)
		p97 := (currBaseline.PriceP97 * weightCurr) + (prevBaseline.PriceP97 * weightPrev)
		pricePct = classifyPercentile(liveDisplacement, p05, p10, p25, p50, p75, p90, p97)

		tc05 := (currBaseline.TickCountP05 * weightCurr) + (prevBaseline.TickCountP05 * weightPrev)
		tc10 := (currBaseline.TickCountP10 * weightCurr) + (prevBaseline.TickCountP10 * weightPrev)
		tc25 := (currBaseline.TickCountP25 * weightCurr) + (prevBaseline.TickCountP25 * weightPrev)
		tc50 := (currBaseline.TickCountP50 * weightCurr) + (prevBaseline.TickCountP50 * weightPrev)
		tc75 := (currBaseline.TickCountP75 * weightCurr) + (prevBaseline.TickCountP75 * weightPrev)
		tc90 := (currBaseline.TickCountP90 * weightCurr) + (prevBaseline.TickCountP90 * weightPrev)
		tc97 := (currBaseline.TickCountP97 * weightCurr) + (prevBaseline.TickCountP97 * weightPrev)
		tickPct = classifyPercentile(float64(liveTickCount), tc05, tc10, tc25, tc50, tc75, tc90, tc97)
	}

	tick.Enrichment = models.EnrichmentState{
		MinuteIndex:      minuteIndex,
		VolumePercentile: volPct,
		PricePercentile:  pricePct,
		TickPercentile:   tickPct,
		Timestamp:        ts,
	}

	// === COMMIT SNAPSHOT ON EACH NEW MINUTE CHECKPOINT ===
	if minuteIndex != ctx.LastSavedIndex {
		snapshot := models.SessionSnapshot{
			Timestamp:    ts,
			MinuteIndex:  minuteIndex,
			VolumeRank:   getPercentileRank(volPct),
			PriceRank:    getPercentileRank(pricePct),
			Displacement: liveDisplacement,
			ClosePrice:   price,
		}

		ctx.SessionCanvas.Timeline = append(ctx.SessionCanvas.Timeline, snapshot)

		// Prevent slice memory drift past maximum cash session limits
		if len(ctx.SessionCanvas.Timeline) > ctx.SessionCanvas.MaxStoredSteps {
			ctx.SessionCanvas.Timeline = ctx.SessionCanvas.Timeline[1:]
		}

		ctx.LastSavedIndex = minuteIndex
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

// GetSessionContext returns a thread-safe context pointer for the analytics engine
func (s *EnrichmentStage) GetSessionContext(token uint32) (*models.SessionContext, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, exists := s.instruments[token]
	if !exists {
		return nil, false
	}
	return ctx.SessionCanvas, true
}

// GetRollingStructure fetches the short-term 60s structure variables for the engine
func (s *EnrichmentStage) GetRollingStructure(token uint32) (vol, rOpen, rHigh, rLow, rClose float64) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, exists := s.instruments[token]
	if !exists || ctx.Buffer == nil {
		return 0, 0, 0, 0, 0
	}

	return ctx.Buffer.GetProductionStructure()
}
