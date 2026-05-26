package pipeline

import (
	"sync"

	"gidh-backend/internal/service/models"
)

type AnalyticsEngine struct {
	mu         sync.Mutex
	enrichment *EnrichmentStage // Direct pointer to query the session-wide timeline array
}

// NewAnalyticsEngine couples analytics with the enrichment environment
func NewAnalyticsEngine(enrichment *EnrichmentStage) *AnalyticsEngine {
	return &AnalyticsEngine{
		enrichment: enrichment,
	}
}

// Analyze evaluates the live context to capture pure participation anomalies
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) models.AnomalySnapshot {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	currentPrice := tick.Raw.LastPrice

	// 1. Extract ungameable linear ranks (1-7 coordinate system) from live telemetry
	volRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	priceRank := getPercentileRank(tick.Enrichment.PricePercentile)

	// 2. Setup the baseline snapshot payload
	snapshot := models.AnomalySnapshot{
		Timestamp:  tick.Enrichment.Timestamp,
		Type:       models.AnomalyNone,
		Direction:  0, // Strictly objective, interpretation-free data
		VolumeRank: volRank,
		PriceRank:  priceRank,
		Price:      currentPrice,
	}

	// 3. Evaluate the Volume Burst Anomaly Threshold
	// Rank 6 matches a P90 spike, Rank 7 matches an extreme P97 non-linear burst
	if volRank >= 6 {
		snapshot.Type = models.AnomalyVolumeBurst

		// 4. Query the Session Timeline array to see how much macro energy has accumulated
		if sessionCtx, exists := ae.enrichment.GetSessionContext(token); exists {
			// Pull total active participation energy over the last 15 steps
			energy15m, _, _ := sessionCtx.CalculateCumulativePressure(15)

			// We can route this rolling energy value to a log or attach it as a confidence metric
			_ = energy15m
		}
	}

	return snapshot
}
