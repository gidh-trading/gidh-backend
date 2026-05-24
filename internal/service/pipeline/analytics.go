package pipeline

import (
	"gidh-backend/internal/service/models"
)

type AnalyticsEngine struct{}

func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{}
}

// Analyze processes the telemetry and returns a type-safe AnomalySnapshot
// based entirely on continuous, bar-independent microstructural metrics.
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) models.AnomalySnapshot {
	volRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	priceRank := getPercentileRank(tick.Enrichment.PricePercentile)
	displacement := tick.Telemetry.LiveDisplacement

	// Initialize a lean, default snapshot footprint targeting our type-safe enums
	snapshot := models.AnomalySnapshot{
		Timestamp:  tick.Enrichment.Timestamp,
		Type:       models.AnomalyNone, // Default to safe fallback state
		Direction:  0,
		VolumeRank: volRank,
		PriceRank:  priceRank,
	}

	// Gatekeeper check: Identify severe institutional activity (P90 = Rank 6, P97 = Rank 7)
	if volRank >= 6 {
		// Assign basic directional breakout attributes to the volume anomaly
		snapshot.Type = models.AnomalyVolumeBurst
		if displacement > 0 {
			snapshot.Direction = 1
		} else if displacement < 0 {
			snapshot.Direction = -1
		}

		// Microstructural Absorption Check: Elevated volume matched with capped/stalled price metrics
		if priceRank <= 3 { // P25 or weaker localized price impact
			snapshot.Type = models.AnomalyAbsorption // Upgrade category classification to compile-safe enum

			// Determine context bias: if price is flat/down on heavy buying pressure vs flat/up on selling
			if displacement >= 0 {
				snapshot.Direction = 1 // Passive supply accumulation (Buy Absorption)
			} else {
				snapshot.Direction = -1 // Passive demand distribution (Sell Absorption)
			}
		}
	}

	return snapshot
}
