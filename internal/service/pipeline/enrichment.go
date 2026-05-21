package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

// EnrichmentStage drives data orchestration mechanics for incoming tick flows.
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

// NewEnrichmentStage constructs an enrichment operator and pre-indexes historical timeline records for optimized retrieval.
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

// Process coordinates processing sequences cleanly across incoming context properties.
func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(s.loc)

	tick.TickVolume = s.calculateTickVolume(token, tick)
	vol := float64(tick.TickVolume)

	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	priceChanged := tick.Raw.LastPrice != s.lastPriceMap[token]
	if priceChanged && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	s.lastPriceMap[token] = price

	buf, exists := s.buffers[token]
	if !exists {
		buf = NewTokenRollingBuffer()
		s.buffers[token] = buf
	}

	// Update active rolling metrics status definitions
	buf.Push(ts, price, vol, ContinuousWindowDuration)

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555 // Market opener timeline index synchronization offset

	liveVolume, liveTickCount := buf.GetStats()
	var volZ, tcZ, rVol float64

	adv, hasAdv := s.advMap[token]
	liveNormVol := 0.0
	if hasAdv && adv > 0 {
		liveNormVol = liveVolume / adv
	}

	// Linearly interpolate baseline values against historical matrices
	if tokenDna, exists := s.dnaMap[token]; exists {
		if currBaseline, ok := tokenDna[minuteIndex]; ok {
			sec := float64(ts.Second())
			prevBaseline := currBaseline

			if minuteIndex > 0 {
				if pb, ok := tokenDna[minuteIndex-1]; ok {
					prevBaseline = pb
				}
			}

			weightCurr := sec / 60.0
			weightPrev := (60.0 - sec) / 60.0

			rollingVolMean := (currBaseline.VolumeMean * weightCurr) + (prevBaseline.VolumeMean * weightPrev)
			rollingTcMean := (currBaseline.TickCountMean * weightCurr) + (prevBaseline.TickCountMean * weightPrev)

			rollingVolVariance := (weightCurr * currBaseline.VolumeStd * currBaseline.VolumeStd) + (weightPrev * prevBaseline.VolumeStd * prevBaseline.VolumeStd)
			rollingVolStd := math.Sqrt(rollingVolVariance)

			rollingTcVariance := (weightCurr * currBaseline.TickCountStd * currBaseline.TickCountStd) + (weightPrev * prevBaseline.TickCountStd * prevBaseline.TickCountStd)
			rollingTcStd := math.Sqrt(rollingTcVariance)

			if rollingVolStd < 0.00001 {
				rollingVolStd = 0.00001
			}
			if rollingTcStd < 1.0 {
				rollingTcStd = 1.0
			}

			if hasAdv && adv > 0 {
				volZ = (liveNormVol - rollingVolMean) / rollingVolStd
				rVol = liveNormVol / rollingVolMean
			}
			tcZ = (float64(liveTickCount) - rollingTcMean) / rollingTcStd
		}
	}

	if rVol == 0 && hasAdv && adv > 0 {
		if expectedVolPerMin := adv / 375.0; expectedVolPerMin > 0 {
			rVol = liveVolume / expectedVolPerMin
		}
	}

	// Populate structural context outcomes downstream
	tick.RelativeVolume = rVol
	tick.VolumeZ = volZ
	tick.TickCountZ = tcZ

	// Compute Experimental Telemetry Dimensions
	if buf.RollingHigh > 0 && buf.RollingLow < math.MaxFloat64 {
		tick.RealizedRange = buf.RollingHigh - buf.RollingLow
	} else {
		tick.RealizedRange = 0.0
	}

	denomVolume := math.Max(1.0, liveVolume)
	tick.Efficiency = tick.RealizedRange / denomVolume

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
