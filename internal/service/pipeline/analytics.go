package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
	"sync"
)

// Move this struct from helpers.go to here
type tokenTickState struct {
	lastPrice    float64
	lastBidPrice float64
	lastBidQty   float64
	lastAskPrice float64
	lastAskQty   float64
}

type AnalyticsStage struct {
	profiles      map[uint32]*models.InstrumentProfile
	lastTickState map[uint32]*tokenTickState
	mu            sync.Mutex
}

func NewAnalyticsStage(profiles map[uint32]*models.InstrumentProfile) *AnalyticsStage {
	return &AnalyticsStage{
		profiles:      profiles,
		lastTickState: make(map[uint32]*tokenTickState),
	}
}

func (s *AnalyticsStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)

	// 1. Anomaly Detection
	bucketSize := 1.0
	if prof, ok := s.profiles[token]; ok && prof.BucketSize > 0 {
		bucketSize = prof.BucketSize
	}

	if tick.VolumeZ > 2.0 && tick.RelativeVolume > 2.5 {
		tick.HasAnomaly = true
		tick.AnomalyBin = math.Floor(price/bucketSize) * bucketSize
	} else {
		tick.HasAnomaly = false
		tick.AnomalyBin = math.Floor(price/bucketSize) * bucketSize // Fallback bin
		if tick.AnomalyBin == 0 {
			tick.AnomalyBin = price
		}
	}

	// 2. Microstructure Analysis (Moved from Bar Manager)
	if s.lastTickState[token] == nil {
		s.lastTickState[token] = &tokenTickState{lastPrice: price}
	}
	lastState := s.lastTickState[token]
	depth := tick.Raw.Depth

	if len(depth.Buy) > 0 && len(depth.Sell) > 0 {
		currBidP, currBidQ := depth.Buy[0].Price, float64(depth.Buy[0].Quantity)
		currAskP, currAskQ := depth.Sell[0].Price, float64(depth.Sell[0].Quantity)

		var ms models.TickMicrostructure

		// A. Trade Classification (Lee-Ready)
		if vol > 0 {
			if price >= currAskP {
				ms.AggressiveBuy = vol
			} else if price <= currBidP {
				ms.AggressiveSell = vol
			} else {
				if price > lastState.lastPrice {
					ms.AggressiveBuy = vol
				} else if price < lastState.lastPrice {
					ms.AggressiveSell = vol
				}
			}
		}

		// B. Resting Depth Metrics
		var wBid, wAsk float64
		for i := 0; i < len(depth.Buy); i++ {
			wBid += float64(depth.Buy[i].Quantity) / float64(i+1)
		}
		for i := 0; i < len(depth.Sell); i++ {
			wAsk += float64(depth.Sell[i].Quantity) / float64(i+1)
		}
		if wBid+wAsk > 0 {
			ms.DepthImbalance = (wBid - wAsk) / (wBid + wAsk)
		}

		ms.MicroPrice = (currAskP*currBidQ + currBidP*currAskQ) / (currBidQ + currAskQ)

		// C. Order Flow Dynamics (Consumption & VOFI)
		if lastState.lastBidPrice > 0 && lastState.lastAskPrice > 0 {
			if currAskP == lastState.lastAskPrice && currAskQ < lastState.lastAskQty {
				ms.ConsumedAsk = lastState.lastAskQty - currAskQ
			}
			if currBidP == lastState.lastBidPrice && currBidQ < lastState.lastBidQty {
				ms.ConsumedBid = lastState.lastBidQty - currBidQ
			}

			// VOFI
			var deltaBid, deltaAsk float64
			if currBidP > lastState.lastBidPrice {
				deltaBid = currBidQ
			} else if currBidP == lastState.lastBidPrice {
				deltaBid = currBidQ - lastState.lastBidQty
			} else {
				deltaBid = -lastState.lastBidQty
			}
			if currAskP < lastState.lastAskPrice {
				deltaAsk = currAskQ
			} else if currAskP == lastState.lastAskPrice {
				deltaAsk = currAskQ - lastState.lastAskQty
			} else {
				deltaAsk = -lastState.lastAskQty
			}

			ms.NormalizedVOFI = (deltaBid - deltaAsk) / math.Max(vol, 1.0)
		}

		// Attach to tick
		tick.Microstructure = ms

		// Update Cache
		lastState.lastPrice = price
		lastState.lastBidPrice, lastState.lastBidQty = currBidP, currBidQ
		lastState.lastAskPrice, lastState.lastAskQty = currAskP, currAskQ
	}

	return nil
}
