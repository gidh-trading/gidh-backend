package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

func classifyPercentile(value, p05, p10, p50, p90, p95, p99 float64) string {
	switch {
	case value >= p99:
		return "P99"
	case value >= p95:
		return "P95"
	case value >= p90:
		return "P90"
	case value >= p50:
		return "P50"
	case value >= p10:
		return "P10"
	case value >= p05:
		return "P05"
	default:
		return "NORMAL"
	}
}

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
	volDelta := float64(tick.TickVolume)

	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	if price != s.lastPriceMap[token] && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	s.lastPriceMap[token] = price

	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}

	buf.Push(ts, price, volDelta)

	liveVolume, liveTickCount, realizedRange, _ := buf.GetProductionMetrics()

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex
	tick.EnrichedAt = time.Now().UnixMilli()

	adv, hasAdv := s.advMap[token]
	if !hasAdv || adv <= 0 {
		adv = 1.0
	}
	rVol := liveVolume / adv

	tick.Telemetry = models.LiveTelemetry{
		MinuteIndex:    minuteIndex,
		TickCount:      liveTickCount,
		RealizedRange:  realizedRange,
		RelativeVolume: rVol,
	}

	rVolPct, rangePct, tickPct := "NORMAL", "NORMAL", "NORMAL"

	if tokenDna, exists := s.dnaMap[token]; exists {
		if currBaseline, ok := tokenDna[minuteIndex]; ok {
			tick.DNASampleCount = currBaseline.SampleCount

			sec := float64(ts.Second())
			prevBaseline := currBaseline
			if minuteIndex > 0 {
				if pb, ok := tokenDna[minuteIndex-1]; ok {
					prevBaseline = pb
				}
			}

			weightCurr := sec / 60.0
			weightPrev := (60.0 - sec) / 60.0

			// 1. Relative Volume Spacing Percentile
			rv05 := (currBaseline.RelativeVolumeP05 * weightCurr) + (prevBaseline.RelativeVolumeP05 * weightPrev)
			rv10 := (currBaseline.RelativeVolumeP10 * weightCurr) + (prevBaseline.RelativeVolumeP10 * weightPrev)
			rv50 := (currBaseline.RelativeVolumeP50 * weightCurr) + (prevBaseline.RelativeVolumeP50 * weightPrev)
			rv90 := (currBaseline.RelativeVolumeP90 * weightCurr) + (prevBaseline.RelativeVolumeP90 * weightPrev)
			rv95 := (currBaseline.RelativeVolumeP95 * weightCurr) + (prevBaseline.RelativeVolumeP95 * weightPrev)
			rv99 := (currBaseline.RelativeVolumeP99 * weightCurr) + (prevBaseline.RelativeVolumeP99 * weightPrev)
			rVolPct = classifyPercentile(rVol, rv05, rv10, rv50, rv90, rv95, rv99)

			// 2. Price Range Spacing Percentile
			r05 := (currBaseline.RangeP05 * weightCurr) + (prevBaseline.RangeP05 * weightPrev)
			r10 := (currBaseline.RangeP10 * weightCurr) + (prevBaseline.RangeP10 * weightPrev)
			r50 := (currBaseline.RangeP50 * weightCurr) + (prevBaseline.RangeP50 * weightPrev)
			r90 := (currBaseline.RangeP90 * weightCurr) + (prevBaseline.RangeP90 * weightPrev)
			r95 := (currBaseline.RangeP95 * weightCurr) + (prevBaseline.RangeP95 * weightPrev)
			r99 := (currBaseline.RangeP99 * weightCurr) + (prevBaseline.RangeP99 * weightPrev)
			rangePct = classifyPercentile(realizedRange, r05, r10, r50, r90, r95, r99)

			// 3. Tick Count Spacing Percentile
			tc05 := (currBaseline.TickCountP05 * weightCurr) + (prevBaseline.TickCountP05 * weightPrev)
			tc10 := (currBaseline.TickCountP10 * weightCurr) + (prevBaseline.TickCountP10 * weightPrev)
			tc50 := (currBaseline.TickCountP50 * weightCurr) + (prevBaseline.TickCountP50 * weightPrev)
			tc90 := (currBaseline.TickCountP90 * weightCurr) + (prevBaseline.TickCountP90 * weightPrev)
			tc95 := (currBaseline.TickCountP95 * weightCurr) + (prevBaseline.TickCountP95 * weightPrev)
			tc99 := (currBaseline.TickCountP99 * weightCurr) + (prevBaseline.TickCountP99 * weightPrev)
			tickPct = classifyPercentile(float64(liveTickCount), tc05, tc10, tc50, tc90, tc95, tc99)
		}
	}

	tick.EnrichmentStr = rVolPct
	tick.Enrichment = models.EnrichmentState{
		MinuteIndex:              minuteIndex,
		RelativeVolumePercentile: rVolPct,
		RangePercentile:          rangePct,
		TickPercentile:           tickPct,
		Timestamp:                ts,
	}

	return nil
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
