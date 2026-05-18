// internal/service/pipeline/enrichment.go
package pipeline

import (
	"gidh-backend/internal/service/models"
	"sync"
)

type EnrichmentStage struct {
	lastVolumeMap map[uint32]int64
	lastPriceMap  map[uint32]float64
	mu            sync.Mutex
}

func NewEnrichmentStage() *EnrichmentStage {
	return &EnrichmentStage{
		lastVolumeMap: make(map[uint32]int64),
		lastPriceMap:  make(map[uint32]float64),
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice

	// 1. Calculate actual tick volume (delta)
	tick.TickVolume = s.calculateTickVolume(token, tick)

	if tick.TickVolume == 0 && price == s.lastPriceMap[token] {
		return nil
	}

	s.lastPriceMap[token] = price
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
