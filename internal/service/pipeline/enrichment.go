// internal/service/pipeline/enrichment.go

package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"sync"
	"time"
)

type EnrichmentStage struct {
	lastVolumeMap   map[uint32]int64
	lastPriceMap    map[uint32]float64
	positionManager order.PositionManager

	loc *time.Location
	mu  sync.RWMutex
}

func NewEnrichmentStage(pm order.PositionManager) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &EnrichmentStage{
		lastVolumeMap:   make(map[uint32]int64),
		lastPriceMap:    make(map[uint32]float64),
		loc:             loc,
		positionManager: pm,
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken

	// Tick volume
	tick.TickVolume = s.calculateTickVolume(token, tick)

	// Skip dead updates
	if tick.TickVolume == 0 && tick.Raw.LastPrice == s.lastPriceMap[token] {
		return nil
	}

	priceChanged := tick.Raw.LastPrice != s.lastPriceMap[token]

	if priceChanged && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
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
		// reset case
		delta = tick.Raw.LastTradedQuantity
	}

	s.lastVolumeMap[token] = curr
	return delta
}
