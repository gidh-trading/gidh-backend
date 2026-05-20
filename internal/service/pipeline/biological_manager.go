package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
	"sync"
)

type PendingCell struct {
	PriceLevel        float64
	InitialPrice      float64
	TriggerDelta      float64
	AccumulatedVolume float64
	AccumulatedBuy    float64
	AccumulatedSell   float64
	TicksSinceLastHit int
	MaxObservedPrice  float64
	MinObservedPrice  float64
	IsClosed          bool
}

type BiologicalStage struct {
	activeCells map[uint32]map[float64]*PendingCell // InstrumentToken -> PriceBin -> CellState
	mu          sync.Mutex
}

func NewBiologicalStage() *BiologicalStage {
	return &BiologicalStage{
		activeCells: make(map[uint32]map[float64]*PendingCell),
	}
}

func (s *BiologicalStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	bin := tick.AnomalyBin
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)

	// Initialize tracking maps for this instrument if fresh
	if _, exists := s.activeCells[token]; !exists {
		s.activeCells[token] = make(map[float64]*PendingCell)
	}

	// 1. Biological Receptor Trigger: Open a cellular channel if an institutional footprint is caught
	if tick.HasAnomaly {
		if _, exists := s.activeCells[token][bin]; !exists {
			s.activeCells[token][bin] = &PendingCell{
				PriceLevel:       bin,
				InitialPrice:     price,
				TriggerDelta:     tick.Microstructure.AggressiveBuy - tick.Microstructure.AggressiveSell,
				MaxObservedPrice: price,
				MinObservedPrice: price,
			}
		}
		// Reset synaptic decay count on active molecular reinforcement
		s.activeCells[token][bin].TicksSinceLastHit = 0
	}

	// 2. Lifecycle Evaluation Loop for open channels
	bucketSize := 1.0
	if tick.FullVolProfile != nil && tick.FullVolProfile.BucketSize > 0 {
		bucketSize = tick.FullVolProfile.BucketSize
	}

	for priceBin, cell := range s.activeCells[token] {
		if cell.IsClosed {
			continue
		}

		// Update spatial extremes within this biological cycle
		if price > cell.MaxObservedPrice {
			cell.MaxObservedPrice = price
		}
		if price < cell.MinObservedPrice {
			cell.MinObservedPrice = price
		}

		cell.AccumulatedVolume += vol
		cell.AccumulatedBuy += tick.Microstructure.AggressiveBuy
		cell.AccumulatedSell += tick.Microstructure.AggressiveSell
		cell.TicksSinceLastHit++

		// -----------------------------------------------------------------
		// ⚡ PATHWAY A: NERVE ACTION POTENTIAL (Instant Initiation Response)
		// -----------------------------------------------------------------
		upwardDisplacement := price - cell.InitialPrice
		downwardDisplacement := cell.InitialPrice - price

		if cell.TriggerDelta > 0 && upwardDisplacement >= (bucketSize*2.0) {
			marker := models.BioEventMarker{
				PriceLevel: cell.PriceLevel,
				EventType:  "INITIATION_UP",
				Intensity:  cell.AccumulatedVolume,
				Timestamp:  tick.Raw.Timestamp,
			}
			tick.ResolvedBioEvents = append(tick.ResolvedBioEvents, marker)
			cell.IsClosed = true
			delete(s.activeCells[token], priceBin)
			continue
		}

		if cell.TriggerDelta < 0 && downwardDisplacement >= (bucketSize*2.0) {
			marker := models.BioEventMarker{
				PriceLevel: cell.PriceLevel,
				EventType:  "INITIATION_DOWN",
				Intensity:  cell.AccumulatedVolume,
				Timestamp:  tick.Raw.Timestamp,
			}
			tick.ResolvedBioEvents = append(tick.ResolvedBioEvents, marker)
			cell.IsClosed = true
			delete(s.activeCells[token], priceBin)
			continue
		}

		// -----------------------------------------------------------------
		// ⏳ PATHWAY B: SYNAPTIC DISSIPATION (Delayed Equilibrium Absorption / Trap)
		// -----------------------------------------------------------------
		// If 50 quiet ticks pass for this token, the institutional energy has faded.
		if cell.TicksSinceLastHit > 50 {
			cell.IsClosed = true
			priceRange := cell.MaxObservedPrice - cell.MinObservedPrice
			netDelta := cell.AccumulatedBuy - cell.AccumulatedSell

			// Evaluate if it held equilibrium or if it was a late-stage trap
			if priceRange <= (bucketSize * 1.5) {
				// High volume saturation with zero structural range expansion = Absorption
				eventType := "BEARISH_ABSORPTION"
				if cell.TriggerDelta < 0 {
					eventType = "BULLISH_ABSORPTION"
				}

				tick.ResolvedBioEvents = append(tick.ResolvedBioEvents, models.BioEventMarker{
					PriceLevel: cell.PriceLevel,
					EventType:  eventType,
					Intensity:  cell.AccumulatedVolume,
					Timestamp:  tick.Raw.Timestamp,
				})
			} else if tick.VolProfile != nil {
				// Spatial verification for exhaustion traps
				isAtVAH := math.Abs(cell.PriceLevel-tick.VolProfile.VAH) <= (bucketSize * 1.5)
				isAtVAL := math.Abs(cell.PriceLevel-tick.VolProfile.VAL) <= (bucketSize * 1.5)

				if cell.TriggerDelta > 0 && netDelta > 0 && isAtVAH && price < cell.MaxObservedPrice {
					tick.ResolvedBioEvents = append(tick.ResolvedBioEvents, models.BioEventMarker{
						PriceLevel: cell.PriceLevel,
						EventType:  "EXHAUSTION_TOP",
						Intensity:  cell.AccumulatedVolume,
						Timestamp:  tick.Raw.Timestamp,
					})
				} else if cell.TriggerDelta < 0 && netDelta < 0 && isAtVAL && price > cell.MinObservedPrice {
					tick.ResolvedBioEvents = append(tick.ResolvedBioEvents, models.BioEventMarker{
						PriceLevel: cell.PriceLevel,
						EventType:  "EXHAUSTION_BOTTOM",
						Intensity:  cell.AccumulatedVolume,
						Timestamp:  tick.Raw.Timestamp,
					})
				}
			}

			// Clean up state machine allocation for this bin context
			delete(s.activeCells[token], priceBin)
		}
	}

	return nil
}
