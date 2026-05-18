// internal/service/pipeline/analytics.go
package pipeline

import (
	"gidh-backend/internal/service/models"
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
	mu            sync.Mutex
}

func NewAnalyticsStage() *AnalyticsStage {
	return &AnalyticsStage{
		lastTickState: make(map[uint32]*tokenTickState),
	}
}

func (s *AnalyticsStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)

	if s.lastTickState[token] == nil {
		s.lastTickState[token] = &tokenTickState{lastPrice: price, lastTickDir: 1}
	}
	lastState := s.lastTickState[token]
	depth := tick.Raw.Depth

	var ms models.TickMicrostructure

	// Classify volume if trades happened and order book exists
	if vol > 0 && len(depth.Buy) > 0 && len(depth.Sell) > 0 {
		currBidP := depth.Buy[0].Price
		currAskP := depth.Sell[0].Price

		// 1. SWEEP DETECTION (Prevents false signals on huge market drops)
		isBuySweep := lastState.lastAskPrice > 0 && price > lastState.lastAskPrice
		isSellSweep := lastState.lastBidPrice > 0 && price < lastState.lastBidPrice

		if isBuySweep {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if isSellSweep {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1

			// 2. STANDARD QUOTE RULE
		} else if price >= currAskP {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if price <= currBidP {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1

			// 3. TICK RULE (Trade happened inside the spread)
		} else if price > lastState.lastPrice {
			ms.AggressiveBuy = vol
			lastState.lastTickDir = 1
		} else if price < lastState.lastPrice {
			ms.AggressiveSell = vol
			lastState.lastTickDir = -1

			// 4. ZERO-TICK RULE (Price is identical to last trade)
		} else {
			if lastState.lastTickDir >= 0 {
				ms.AggressiveBuy = vol
			} else {
				ms.AggressiveSell = vol
			}
		}

		// Update resting quotes for next tick's sweep detection
		lastState.lastBidPrice = currBidP
		lastState.lastAskPrice = currAskP
	}

	// Attach to tick and update last price
	tick.Microstructure = ms
	lastState.lastPrice = price

	return nil
}
