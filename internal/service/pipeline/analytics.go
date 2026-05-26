package pipeline

import (
	"gidh-backend/internal/service/models"
)

type AnalyticsEngine struct{}

// NewAnalyticsEngine initializes a simplified participation analytics component
func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{}
}

// Analyze evaluates the simplified context to instantly flag volume burst anomalies
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) models.AnomalySnapshot {
	// Extract the simple 1-7 coordinate rank directly from the enrichment metadata
	volRank := tick.Enrichment.VolumeRank

	// Setup the base snapshot payload
	snapshot := models.AnomalySnapshot{
		Timestamp:  tick.Enrichment.Timestamp,
		Type:       models.AnomalyNone,
		VolumeRank: volRank,
		Price:      tick.Raw.LastPrice,
	}

	// Any execution activity tracking at or above Rank 6 (P90 elevated/burst benchmarks)
	// triggers an objective, interpretation-free Volume Burst Anomaly
	if volRank >= 6 {
		snapshot.Type = models.AnomalyVolumeBurst
	}

	return snapshot
}
