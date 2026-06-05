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
	latest3mDir     map[string]models.DirectionState // Stores the latest 3-minute bar direction
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		activePositions: make(map[string]*PositionRisk),
		latest3mDir:     make(map[string]models.DirectionState),
	}
}

// ========================================================================
// 1. TICK ANALYZER (Hunts for Entries)
// ========================================================================
func (sa *ScalperAgent) AnalyzeTickForEntry(tick *models.EnrichedTick) (string, bool) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	symbol := tick.Raw.StockName

	if sa.activePositions[symbol] == nil {
		sa.activePositions[symbol] = &PositionRisk{IsActive: false}
	}
	pos := sa.activePositions[symbol]

	// If we are already in a trade, ignore ticks. We use 3m bars for exits now.
	if pos.IsActive {
		return "", false
	}

	// ENTRY RULE: Institutional footprint detected on 1m rolling window
	if tick.Enrichment.VolumeRank >= 6 && tick.Enrichment.PriceRank >= 6 {
		direction := tick.Enrichment.Direction

		if direction == models.DirStrongBullish || direction == models.DirBullish {
			pos.IsActive = true
			pos.Side = "LONG"
			pos.EntryPrice = tick.Raw.LastPrice
			return "GO_LONG", true
		}

		if direction == models.DirStrongBearish || direction == models.DirBearish {
			pos.IsActive = true
			pos.Side = "SHORT"
			pos.EntryPrice = tick.Raw.LastPrice
			return "GO_SHORT", true
		}
	}

	return "", false
}

// ========================================================================
// 2. BAR ANALYZER (Hunts for Exits)
// ========================================================================
func (sa *ScalperAgent) UpdateBarDirectionAndCheckExit(symbol string, timeframe string, direction models.DirectionState) (string, bool) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	// We only care about 3-minute bars for trade management
	if timeframe != "3m" {
		return "", false
	}

	// Store the latest 3m direction
	sa.latest3mDir[symbol] = direction

	pos := sa.activePositions[symbol]
	if pos == nil || !pos.IsActive {
		return "", false
	}

	// EXIT RULE: The 3-minute trend turns against our position
	if pos.Side == "LONG" {
		if direction == models.DirStrongBearish || direction == models.DirBearish {
			pos.IsActive = false
			return "EXIT_LONG", true
		}
	}

	if pos.Side == "SHORT" {
		if direction == models.DirStrongBullish || direction == models.DirBullish {
			pos.IsActive = false
			return "EXIT_SHORT", true
		}
	}

	return "", false
}

func (sa *ScalperAgent) GetPositionState(stock string) *PositionRisk {
	sa.mu.RLock()
	defer sa.mu.RUnlock()
	return sa.activePositions[stock]
}

func (sa *ScalperAgent) ResetPositionState(stock string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if sa.activePositions[stock] != nil {
		sa.activePositions[stock].IsActive = false
	}
}
