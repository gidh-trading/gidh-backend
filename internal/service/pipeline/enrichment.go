package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

// classifyPercentile safely maps a raw value against its historical T-Digest bounds
func classifyPercentile(value, p50, p90, p95, p99 float64) string {
	switch {
	case value >= p99:
		return "P99"
	case value >= p95:
		return "P95"
	case value >= p90:
		return "P90"
	case value >= p50:
		return "P50"
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

	// 1. Pull 60s continuous window parameters
	liveVolume, liveTickCount, realizedRange, realizedVolatility := buf.GetProductionMetrics()

	// Market Opener Alignment Index Math Mapping
	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex
	tick.EnrichedAt = time.Now().UnixMilli()

	adv, hasAdv := s.advMap[token]
	if !hasAdv || adv <= 0 {
		adv = 1.0 // Prevent structural division problems
	}

	liveNormVol := liveVolume / adv

	// 2. Hydrate Live Telemetry State (Pure Measurements)
	tick.Telemetry = models.LiveTelemetry{
		MinuteIndex:        minuteIndex,
		Volume:             liveNormVol,
		TickCount:          liveTickCount,
		RealizedRange:      realizedRange,
		RealizedVolatility: realizedVolatility,
	}

	// 3. Defaults if DNA is missing
	var volZ, tcZ, rVolZ float64
	rangePct, effPct := "NORMAL", "NORMAL"
	var partScore float64

	if tokenDna, exists := s.dnaMap[token]; exists {
		if currBaseline, ok := tokenDna[minuteIndex]; ok {
			tick.DNASampleCount = currBaseline.SampleCount

			// Second-by-second continuous window temporal morphing
			sec := float64(ts.Second())
			prevBaseline := currBaseline
			if minuteIndex > 0 {
				if pb, ok := tokenDna[minuteIndex-1]; ok {
					prevBaseline = pb
				}
			}

			weightCurr := sec / 60.0
			weightPrev := (60.0 - sec) / 60.0

			// Morph Participation Baselines (Means & Variances)
			volMean := (currBaseline.VolumeMean * weightCurr) + (prevBaseline.VolumeMean * weightPrev)
			volStd := math.Sqrt((weightCurr * currBaseline.VolumeStd * currBaseline.VolumeStd) + (weightPrev * prevBaseline.VolumeStd * prevBaseline.VolumeStd))

			tcMean := (currBaseline.TickCountMean * weightCurr) + (prevBaseline.TickCountMean * weightPrev)
			tcStd := math.Sqrt((weightCurr * currBaseline.TickCountStd * currBaseline.TickCountStd) + (weightPrev * prevBaseline.TickCountStd * prevBaseline.TickCountStd))

			rVolMean := (currBaseline.RelativeVolumeMean * weightCurr) + (prevBaseline.RelativeVolumeMean * weightPrev)
			rVolStd := math.Sqrt((weightCurr * currBaseline.RelativeVolumeStd * currBaseline.RelativeVolumeStd) + (weightPrev * prevBaseline.RelativeVolumeStd * prevBaseline.RelativeVolumeStd))

			// Calculate Live Relative Volume
			rVol := 0.0
			if volMean > 0 {
				rVol = liveNormVol / volMean
			}
			tick.Telemetry.RelativeVolume = rVol

			// Execute Z-Score Participation Formulations (Using Max to prevent infinity)
			volZ = (liveNormVol - volMean) / math.Max(volStd, 1e-5)
			tcZ = (float64(liveTickCount) - tcMean) / math.Max(tcStd, 1e-5)
			rVolZ = (rVol - rVolMean) / math.Max(rVolStd, 1e-5)

			// Minimum Volume Constraint Check for Efficiency
			const MinimumVolumeThreshold = 10.0
			var efficiency float64

			if liveVolume >= MinimumVolumeThreshold {
				denom := math.Max(rVol, 1e-9)
				efficiency = realizedVolatility / denom
				tick.Telemetry.Efficiency = efficiency

				// Morph non-Gaussian distribution threshold percentiles
				e50 := (currBaseline.EfficiencyP50 * weightCurr) + (prevBaseline.EfficiencyP50 * weightPrev)
				e90 := (currBaseline.EfficiencyP90 * weightCurr) + (prevBaseline.EfficiencyP90 * weightPrev)
				e95 := (currBaseline.EfficiencyP95 * weightCurr) + (prevBaseline.EfficiencyP95 * weightPrev)
				e99 := (currBaseline.EfficiencyP99 * weightCurr) + (prevBaseline.EfficiencyP99 * weightPrev)

				effPct = classifyPercentile(efficiency, e50, e90, e95, e99)
			}

			// Morph Range Percentiles
			r50 := (currBaseline.RangeP50 * weightCurr) + (prevBaseline.RangeP50 * weightPrev)
			r90 := (currBaseline.RangeP90 * weightCurr) + (prevBaseline.RangeP90 * weightPrev)
			r95 := (currBaseline.RangeP95 * weightCurr) + (prevBaseline.RangeP95 * weightPrev)
			r99 := (currBaseline.RangeP99 * weightCurr) + (prevBaseline.RangeP99 * weightPrev)

			rangePct = classifyPercentile(realizedRange, r50, r90, r95, r99)

			// 4. Calculate Master Anomaly Meter
			partScore = (math.Abs(volZ) * 0.5) + (math.Abs(tcZ) * 0.3) + (math.Abs(rVolZ) * 0.2)
		}
	}

	// 5. Finalize the Enrichment State Projection
	tick.Enrichment = models.EnrichmentState{
		MinuteIndex:          minuteIndex,
		VolumeZ:              volZ,
		TickZ:                tcZ,
		RelativeVolumeZ:      rVolZ,
		RangePercentile:      rangePct,
		EfficiencyPercentile: effPct,
		ParticipationScore:   partScore,
		IsVolumeExtreme:      math.Abs(volZ) >= 2.0,
		IsTickExtreme:        math.Abs(tcZ) >= 2.0,
		IsRangeExtreme:       rangePct == "P95" || rangePct == "P99",
		Timestamp:            ts,
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
