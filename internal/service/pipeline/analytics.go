// internal/service/pipeline/analytics.go
package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
	"sync"
)

type tokenTickState struct {
	lastPrice    float64
	lastTickDir  int // +1 for Uptick/Buy, -1 for Downtick/Sell
	lastBidPrice float64
	lastAskPrice float64
}

type AnalyticsStage struct {
	lastTickState map[uint32]*tokenTickState
	bucketSizes   map[uint32]float64
	mu            sync.Mutex
}

func NewAnalyticsStage(bucketSizes map[uint32]float64) *AnalyticsStage {
	if bucketSizes == nil {
		bucketSizes = make(map[uint32]float64)
	}
	return &AnalyticsStage{
		lastTickState: make(map[uint32]*tokenTickState),
		bucketSizes:   bucketSizes,
	}
}

func (s *AnalyticsStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)

	// --- 1. ANOMALY DETECTION & PRICE BINNING ---
	bucketSize := 1.0 // Fallback default
	if bs, exists := s.bucketSizes[token]; exists && bs > 0 {
		bucketSize = bs // Use the dynamic stock-specific bucket size
	}

	tick.AnomalyBin = math.Floor(price/bucketSize) * bucketSize

	// 🕵️ THE DUAL-ANOMALY ENGINE
	// 1. Volume Anomaly (The Whale): Massive volume spikes compared to the 30-day average.
	isVolumeAnomaly := tick.VolumeZ > 2.0 && tick.RelativeVolume > 1.5

	// 2. Frequency Anomaly (The Stealth Iceberg): Transaction counts are +2 StdDevs above the historical mean for this specific minute.
	isTickFrequencyAnomaly := tick.TickCountZ > 1.5 && tick.RelativeVolume > 1.0

	// Trigger the heatmap rendering if EITHER of these institutional footprints are detected
	if isVolumeAnomaly || isTickFrequencyAnomaly {
		tick.HasAnomaly = true
	} else {
		tick.HasAnomaly = false
	}

	// --- 2. SWEEP-AWARE LEE-READY CLASSIFICATION ---
	if s.lastTickState[token] == nil {
		s.lastTickState[token] = &tokenTickState{lastPrice: price, lastTickDir: 1}
	}
	lastState := s.lastTickState[token]
	depth := tick.Raw.Depth
	var ms models.TickMicrostructure

	if vol > 0 && len(depth.Buy) > 0 && len(depth.Sell) > 0 {
		currBidP := depth.Buy[0].Price
		currAskP := depth.Sell[0].Price

		isBuySweep := lastState.lastAskPrice > 0 && price > lastState.lastAskPrice
		isSellSweep := lastState.lastBidPrice > 0 && price < lastState.lastBidPrice

		if isBuySweep {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if isSellSweep {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1
		} else if price >= currAskP {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if price <= currBidP {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1
		} else if price > lastState.lastPrice {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if price < lastState.lastPrice {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1
		} else { // Zero-tick
			if lastState.lastTickDir >= 0 {
				ms.AggressiveBuy = vol
			} else {
				ms.AggressiveSell = vol
			}
		}

		lastState.lastBidPrice = currBidP
		lastState.lastAskPrice = currAskP
	}

	tick.Microstructure = ms
	lastState.lastPrice = price

	return nil
}
