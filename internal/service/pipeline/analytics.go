package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

type AnalyticsEngine struct{}

func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{}
}

func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick, rOpen, rHigh, rLow, rClose float64) models.AnomalySnapshot {
	volRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	priceRank := getPercentileRank(tick.Enrichment.PricePercentile)
	netDisplacement := tick.Telemetry.LiveDisplacement

	snapshot := models.AnomalySnapshot{
		Timestamp:  tick.Enrichment.Timestamp,
		Type:       models.AnomalyNone,
		Direction:  0,
		VolumeRank: volRank,
		PriceRank:  priceRank,
		Price:      tick.Raw.LastPrice,
	}

	if volRank >= 6 {
		// 1. Set default breakout behavior based on net 60s direction
		snapshot.Type = models.AnomalyVolumeBurst
		if netDisplacement > 0 {
			snapshot.Direction = 1
		} else if netDisplacement < 0 {
			snapshot.Direction = -1
		}

		// 2. Compute the Rolling Structural Proportions
		totalRange := rHigh - rLow
		if totalRange > 0 {
			// Calculate how close the current price is to the absolute top/bottom of the 60s window
			upperWickPct := (rHigh - math.Max(rOpen, rClose)) / totalRange
			lowerWickPct := (math.Min(rOpen, rClose) - rLow) / totalRange

			// Look for low price progress relative to the volume being expended:
			// Condition A: Classic Low Displacement (Price Rank 1-3)
			isStalled := priceRank <= 3

			// Condition B: Structural Exhaustion (High Volume, High Range, but significant pull-back)
			// Sellers tried to smash the price down (creating a lower wick), but buyers caught it all.
			isLowerRejection := lowerWickPct >= 0.40 && netDisplacement <= 0
			// Buyers tried to push the price up (creating an upper wick), but sellers blocked it.
			isUpperRejection := upperWickPct >= 0.40 && netDisplacement >= 0

			if isStalled || isLowerRejection || isUpperRejection {
				snapshot.Type = models.AnomalyAbsorption

				if isLowerRejection || (isStalled && netDisplacement >= 0) {
					snapshot.Direction = 1 // Buy Absorption (Passive buyers catching liquid supply)
				} else if isUpperRejection || (isStalled && netDisplacement < 0) {
					snapshot.Direction = -1 // Sell Absorption (Passive sellers capping liquid demand)
				}
			}
		}
	}

	return snapshot
}
