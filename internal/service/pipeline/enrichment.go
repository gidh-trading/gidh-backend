package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

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

	volZPct, rVolPct, rangePct, tickPct := "NORMAL", "NORMAL", "NORMAL", "NORMAL"

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

			// 1A. Absolute Volume Z-Space Percentile
			v05 := (currBaseline.VolumeP05 * weightCurr) + (prevBaseline.VolumeP05 * weightPrev)
			v10 := (currBaseline.VolumeP10 * weightCurr) + (prevBaseline.VolumeP10 * weightPrev)
			v25 := (currBaseline.VolumeP25 * weightCurr) + (prevBaseline.VolumeP25 * weightPrev)
			v50 := (currBaseline.VolumeP50 * weightCurr) + (prevBaseline.VolumeP50 * weightPrev)
			v75 := (currBaseline.VolumeP75 * weightCurr) + (prevBaseline.VolumeP75 * weightPrev)
			v90 := (currBaseline.VolumeP90 * weightCurr) + (prevBaseline.VolumeP90 * weightPrev)
			v97 := (currBaseline.VolumeP97 * weightCurr) + (prevBaseline.VolumeP97 * weightPrev)
			volZPct = classifyPercentile(liveVolume, v05, v10, v25, v50, v75, v90, v97)

			// 1B. Relative Volume Spacing Percentile
			rv05 := (currBaseline.RelativeVolumeP05 * weightCurr) + (prevBaseline.RelativeVolumeP05 * weightPrev)
			rv10 := (currBaseline.RelativeVolumeP10 * weightCurr) + (prevBaseline.RelativeVolumeP10 * weightPrev)
			rv25 := (currBaseline.RelativeVolumeP25 * weightCurr) + (prevBaseline.RelativeVolumeP25 * weightPrev)
			rv50 := (currBaseline.RelativeVolumeP50 * weightCurr) + (prevBaseline.RelativeVolumeP50 * weightPrev)
			rv75 := (currBaseline.RelativeVolumeP75 * weightCurr) + (prevBaseline.RelativeVolumeP75 * weightPrev)
			rv90 := (currBaseline.RelativeVolumeP90 * weightCurr) + (prevBaseline.RelativeVolumeP90 * weightPrev)
			rv97 := (currBaseline.RelativeVolumeP97 * weightCurr) + (prevBaseline.RelativeVolumeP97 * weightPrev)
			rVolPct = classifyPercentile(rVol, rv05, rv10, rv25, rv50, rv75, rv90, rv97)

			// 2. Price Range Spacing Percentile
			r05 := (currBaseline.RangeP05 * weightCurr) + (prevBaseline.RangeP05 * weightPrev)
			r10 := (currBaseline.RangeP10 * weightCurr) + (prevBaseline.RangeP10 * weightPrev)
			r25 := (currBaseline.RangeP25 * weightCurr) + (prevBaseline.RangeP25 * weightPrev)
			r50 := (currBaseline.RangeP50 * weightCurr) + (prevBaseline.RangeP50 * weightPrev)
			r75 := (currBaseline.RangeP75 * weightCurr) + (prevBaseline.RangeP75 * weightPrev)
			r90 := (currBaseline.RangeP90 * weightCurr) + (prevBaseline.RangeP90 * weightPrev)
			r97 := (currBaseline.RangeP97 * weightCurr) + (prevBaseline.RangeP97 * weightPrev)
			rangePct = classifyPercentile(realizedRange, r05, r10, r25, r50, r75, r90, r97)

			// 3. Tick Count Spacing Percentile
			tc05 := (currBaseline.TickCountP05 * weightCurr) + (prevBaseline.TickCountP05 * weightPrev)
			tc10 := (currBaseline.TickCountP10 * weightCurr) + (prevBaseline.TickCountP10 * weightPrev)
			tc25 := (currBaseline.TickCountP25 * weightCurr) + (prevBaseline.TickCountP25 * weightPrev)
			tc50 := (currBaseline.TickCountP50 * weightCurr) + (prevBaseline.TickCountP50 * weightPrev)
			tc75 := (currBaseline.TickCountP75 * weightCurr) + (prevBaseline.TickCountP75 * weightPrev)
			tc90 := (currBaseline.TickCountP90 * weightCurr) + (prevBaseline.TickCountP90 * weightPrev)
			tc97 := (currBaseline.TickCountP97 * weightCurr) + (prevBaseline.TickCountP97 * weightPrev)
			tickPct = classifyPercentile(float64(liveTickCount), tc05, tc10, tc25, tc50, tc75, tc90, tc97)
		}
	}

	tick.EnrichmentStr = rVolPct
	tick.Enrichment = models.EnrichmentState{
		MinuteIndex:              minuteIndex,
		VolumeZPercentile:        volZPct,
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
