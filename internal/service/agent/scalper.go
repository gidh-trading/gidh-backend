package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
)

type PositionRisk struct {
	IsActive   bool
	Side       string // "LONG" or "SHORT"
	EntryPrice float64
}

type ScalperAgent struct {
	mu              sync.RWMutex
	activePositions map[string]*PositionRisk
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		activePositions: make(map[string]*PositionRisk),
	}
}

// ProcessRollingBar handles closed bar intervals pushed from the pipeline
func (sa *ScalperAgent) ProcessRollingBar(symbol string, timeframe string, analytics models.BarAnalytics, currentPrice float64) (string, bool) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.activePositions[symbol] == nil {
		sa.activePositions[symbol] = &PositionRisk{IsActive: false}
	}
	pos := sa.activePositions[symbol]

	// ------------------------------------------------------------------------
	// 🛫 ENTRY LOGIC: Evaluated strictly on the 1-Minute Rolling Bar Close
	// ------------------------------------------------------------------------
	if !pos.IsActive && timeframe == "1m" {
		if analytics.VolumeRank >= 6 && analytics.PriceRank >= 6 { // Institutional footprint confirmed[cite: 5]
			direction := analytics.Direction

			if direction == models.DirStrongBullish || direction == models.DirBullish {
				pos.IsActive = true
				pos.Side = "LONG"
				pos.EntryPrice = currentPrice
				return "GO_LONG", true
			}

			if direction == models.DirStrongBearish || direction == models.DirBearish {
				pos.IsActive = true
				pos.Side = "SHORT"
				pos.EntryPrice = currentPrice
				return "GO_SHORT", true
			}
		}
		return "", false
	}

	// ------------------------------------------------------------------------
	// 🛬 STOP LOSS LOGIC: Evaluated strictly on the 3-Minute Rolling Bar Close
	// ------------------------------------------------------------------------
	if pos.IsActive && timeframe == "3m" {
		direction := analytics.Direction

		if pos.Side == "LONG" {
			// Structural Stop Loss: Trend invalidation on 3m bar
			if direction == models.DirBearish || direction == models.DirStrongBearish || direction == models.DirBearishAbsorption {
				pos.IsActive = false
				return "EXIT_LONG", true
			}
		}

		if pos.Side == "SHORT" {
			// Structural Stop Loss: Trend invalidation on 3m bar
			if direction == models.DirBullish || direction == models.DirStrongBullish || direction == models.DirBullishAbsorption {
				pos.IsActive = false
				return "EXIT_SHORT", true
			}
		}
	}

	return "", false
}

func (sa *ScalperAgent) ResetPositionState(symbol string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if sa.activePositions[symbol] != nil {
		sa.activePositions[symbol].IsActive = false
	}
}
