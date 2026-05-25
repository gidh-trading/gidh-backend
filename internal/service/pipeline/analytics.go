package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
	"sync"
)

type LevelState int

const (
	StateIdle LevelState = iota
	StateTesting
)

type PendingBattlefield struct {
	State        LevelState
	PriceFloor   float64 // Original window bottom
	PriceCeiling float64 // Original window top
	VolumeRank   int
	Direction    int // Initial aggressive push direction (-1 = Aggressive Sellers, 1 = Aggressive Buyers)
}

type AnalyticsEngine struct {
	mu             sync.Mutex
	pendingBattles map[uint32]*PendingBattlefield
}

func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{
		pendingBattles: make(map[uint32]*PendingBattlefield),
	}
}

func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick, rOpen, rHigh, rLow, rClose float64) models.AnomalySnapshot {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	volRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	priceRank := getPercentileRank(tick.Enrichment.PricePercentile)
	netDisplacement := tick.Telemetry.LiveDisplacement
	currentPrice := tick.Raw.LastPrice

	battle, exists := ae.pendingBattles[token]
	if !exists {
		battle = &PendingBattlefield{State: StateIdle}
		ae.pendingBattles[token] = battle
	}

	snapshot := models.AnomalySnapshot{
		Timestamp:  tick.Enrichment.Timestamp,
		Type:       models.AnomalyNone,
		Direction:  0,
		VolumeRank: volRank,
		PriceRank:  priceRank,
		Price:      currentPrice,
	}

	// --- PHASE 1: IDLE STATE ---
	if battle.State == StateIdle {
		if volRank >= 6 { // Extreme Volume Spike (>= P90)
			battle.State = StateTesting
			battle.PriceFloor = rLow
			battle.PriceCeiling = rHigh
			battle.VolumeRank = volRank

			if netDisplacement > 0 {
				battle.Direction = 1 // Aggressive Buyers trying to expand price
			} else {
				battle.Direction = -1 // Aggressive Sellers trying to compress price
			}

			snapshot.Type = models.AnomalyVolumeBurst
			snapshot.Direction = battle.Direction
			return snapshot
		}
		return snapshot
	}

	// --- PHASE 2: TESTING STATE (Continuous structural wick verification) ---
	if battle.State == StateTesting {
		totalRange := rHigh - rLow
		if totalRange <= 0 {
			return snapshot
		}

		upperWickPct := (rHigh - math.Max(rOpen, rClose)) / totalRange
		lowerWickPct := (math.Min(rOpen, rClose) - rLow) / totalRange

		// Case A: Initial push was an aggressive buy. We watch for Passive Sellers to win.
		if battle.Direction == 1 {
			// If a significant upper wick rejection forms, or price stalls completely out
			if upperWickPct >= 0.45 || (priceRank <= 3 && currentPrice < battle.PriceCeiling) {
				snapshot.Type = models.AnomalyAbsorption
				snapshot.Direction = -1 // Passive Sellers Won (Resistance Level Established)
				snapshot.VolumeRank = battle.VolumeRank
				snapshot.Price = battle.PriceCeiling // The level is mapped exactly to the ceiling edge

				battle.State = StateIdle // Reset tracking loop
				return snapshot
			}

			// If buyers smoothly continue past the ceiling with low friction, drop tracking seamlessly
			if currentPrice > battle.PriceCeiling && upperWickPct < 0.15 {
				battle.State = StateIdle
			}
		}

		// Case B: Initial push was an aggressive sell. We watch for Passive Buyers to win.
		if battle.Direction == -1 {
			// If a significant lower wick rejection forms, or price stalls completely out
			if lowerWickPct >= 0.45 || (priceRank <= 3 && currentPrice > battle.PriceFloor) {
				snapshot.Type = models.AnomalyAbsorption
				snapshot.Direction = 1 // Passive Buyers Won (Support Level Established)
				snapshot.VolumeRank = battle.VolumeRank
				snapshot.Price = battle.PriceFloor // The level is mapped exactly to the floor edge

				battle.State = StateIdle // Reset tracking loop
				return snapshot
			}

			// If sellers smoothly breakdown past the floor with low friction, drop tracking seamlessly
			if currentPrice < battle.PriceFloor && lowerWickPct < 0.15 {
				battle.State = StateIdle
			}
		}
	}

	return snapshot
}
