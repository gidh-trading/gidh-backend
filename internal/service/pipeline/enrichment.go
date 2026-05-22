package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

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

	// Pull 60s continuous window parameters
	liveVolume, liveTickCount, realizedRange, realizedVolatility := buf.GetProductionMetrics()

	// Assign raw structures
	tick.Participation.TickVolume = int64(liveVolume)
	tick.Participation.TickCount = liveTickCount
	tick.Response.RealizedRange = realizedRange
	tick.Response.RealizedVolatility = realizedVolatility

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
	rVol := 0.0

	// Default Fallback Baselines
	var volZ, tcZ, rVolZ float64
	effPct := 50.0 // Default center band empirical value fallback

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

			// Morph Participation Baselines
			volMean := (currBaseline.VolumeMean * weightCurr) + (prevBaseline.VolumeMean * weightPrev)
			volStd := math.Sqrt((weightCurr * currBaseline.VolumeStd * currBaseline.VolumeStd) + (weightPrev * prevBaseline.VolumeStd * prevBaseline.VolumeStd))

			tcMean := (currBaseline.TickCountMean * weightCurr) + (prevBaseline.TickCountMean * weightPrev)
			tcStd := math.Sqrt((weightCurr * currBaseline.TickCountStd * currBaseline.TickCountStd) + (weightPrev * prevBaseline.TickCountStd * prevBaseline.TickCountStd))

			rVolMean := (currBaseline.RelativeVolumeMean * weightCurr) + (prevBaseline.RelativeVolumeMean * weightPrev)
			rVolStd := math.Sqrt((weightCurr * currBaseline.RelativeVolumeStd * currBaseline.RelativeVolumeStd) + (weightPrev * prevBaseline.RelativeVolumeStd * prevBaseline.RelativeVolumeStd))

			// Prevent bounds explosions inside volatility metrics
			if volStd < 0.00001 {
				volStd = 0.00001
			}
			if tcStd < 1.0 {
				tcStd = 1.0
			}
			if rVolStd < 0.00001 {
				rVolStd = 0.00001
			}

			// Execute Z-Score Participation Formulations
			volZ = (liveNormVol - volMean) / volStd
			tcZ = (float64(liveTickCount) - tcMean) / tcStd

			if volMean > 0 {
				rVol = liveNormVol / volMean
			}
			rVolZ = (rVol - rVolMean) / rVolStd

			// Morph non-Gaussian distribution threshold percentiles for precise empirical mapping
			p50 := (currBaseline.EfficiencyP50 * weightCurr) + (prevBaseline.EfficiencyP50 * weightPrev)
			p90 := (currBaseline.EfficiencyP90 * weightCurr) + (prevBaseline.EfficiencyP90 * weightPrev)
			p95 := (currBaseline.EfficiencyP95 * weightCurr) + (prevBaseline.EfficiencyP95 * weightPrev)
			p99 := (currBaseline.EfficiencyP99 * weightCurr) + (prevBaseline.EfficiencyP99 * weightPrev)

			// Minimum Volume Constraint Check to isolate thin liquidity artifacts
			const MinimumVolumeThreshold = 10.0
			var efficiency float64

			if liveVolume >= MinimumVolumeThreshold {
				denom := math.Max(rVol, 1e-9) // Production denominator structural adjustment limit
				efficiency = realizedVolatility / denom
				tick.Response.Efficiency = efficiency

				// Coarse-band contextual distribution mapping rank evaluation pass
				switch {
				case efficiency >= p99:
					effPct = 99.0 + (1.0 * (efficiency - p99) / (efficiency + 1.0)) // Asymptotic scale near boundary ceiling
				case efficiency >= p95:
					effPct = 95.0 + 4.0*(efficiency-p95)/math.Max(0.001, p99-p95)
				case efficiency >= p90:
					effPct = 90.0 + 5.0*(efficiency-p90)/math.Max(0.001, p95-p90)
				case efficiency >= p50:
					effPct = 50.0 + 40.0*(efficiency-p50)/math.Max(0.001, p90-p50)
				default:
					effPct = 50.0 * (efficiency / math.Max(0.001, p50))
				}

				if effPct > 100.0 {
					effPct = 100.0
				}
				if effPct < 0.0 {
					effPct = 0.0
				}
			}
		}
	}

	// Hydrate output maps
	tick.Participation.VolumeZ = volZ
	tick.Participation.TickCountZ = tcZ
	tick.Participation.RelativeVolumeZ = rVolZ
	tick.Response.EfficiencyPct = effPct

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
