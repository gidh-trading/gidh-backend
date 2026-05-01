package pipeline

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

type EnrichmentStage struct {
	dnaMap        map[uint32]*models.MarketDNA
	lastVolumeMap map[uint32]int64
	lastPriceMap  map[uint32]float64

	loc *time.Location
	mu  sync.RWMutex
}

func NewEnrichmentStage(dnaMap map[uint32]*models.MarketDNA) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &EnrichmentStage{
		dnaMap:        dnaMap,
		lastVolumeMap: make(map[uint32]int64),
		lastPriceMap:  make(map[uint32]float64),
		loc:           loc,
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken

	// -------------------------
	// 1. Attach DNA + Time Key
	// -------------------------
	if dna, ok := s.dnaMap[token]; ok {
		tick.DNA = dna
	}

	// -------------------------
	// 2. Tick Volume
	// -------------------------
	tick.TickVolume = s.calculateTickVolume(token, tick)

	// skip dead feed updates
	if tick.TickVolume == 0 && tick.Raw.LastPrice == s.lastPriceMap[token] {
		return nil
	}

	s.lastPriceMap[token] = tick.Raw.LastPrice

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
		// Session reset / bad feed reset
		delta = tick.Raw.LastTradedQuantity
	}

	s.lastVolumeMap[token] = curr
	return delta
}
