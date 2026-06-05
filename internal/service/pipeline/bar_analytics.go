// internal/service/pipeline/bar_analytics.go
package pipeline

import (
	"gidh-backend/internal/service/models"
)

type BarAnalyticsEngine struct {
	// We can pass a pointer to dnaMap if we ever want to recalculate ranks inside analytics,
	// but since ranks are already provided inside tick.Enrichment, we can simply aggregate them.
}

func NewBarAnalyticsEngine() *BarAnalyticsEngine {
	return &BarAnalyticsEngine{}
}

// AnalyzeTick updates the candle analytics layer on every snapshot tick update
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	// 1. Accumulate PEAK Intensities for cumulative transaction metrics
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	// 2. 🔥 LIVE STATE OVERWRITE: Price, Range, and Direction show the exact current window reality
	bar.Analytics.PriceRank = tick.Enrichment.PriceRank
	bar.Analytics.RangeRank = tick.Enrichment.RangeRank
	bar.Analytics.Direction = tick.Enrichment.Direction
}

// AnalyzeClose applies final post-processing filters right before archiving the bar segment
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {
	// Open for future finalization or feature compression filters on close boundaries
}
