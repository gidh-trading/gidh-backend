// internal/service/pipeline/bar_analytics.go
package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

type BarAnalyticsEngine struct {
	dnaMap map[uint32]*models.MarketDNA
}

func NewBarAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA) *BarAnalyticsEngine {
	return &BarAnalyticsEngine{
		dnaMap: dnaMap,
	}
}

// AnalyzeTick now ONLY aggregates continuous volume and tick peak ranks
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {
	// 1. Accumulate PEAK Intensities for cumulative transaction metrics
	if tick.Enrichment.VolumeRank > bar.Analytics.VolumeRank {
		bar.Analytics.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > bar.Analytics.TickRank {
		bar.Analytics.TickRank = tick.Enrichment.TickRank
	}

	// 2. Overwrite Direction State from the rolling 60s micro-trigger
	bar.Analytics.Direction = tick.Enrichment.Direction

	// 3. 🔥 LIVE STATE OVERWRITE: Compute macro ranks relative to this specific bar's timeframe DNA
	bae.computeTimeframeRanks(bar)
}

// AnalyzeClose applies final safety guards right before archiving the bar segment
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {
	if bar.Analytics.Direction == "" {
		bar.Analytics.Direction = models.DirSideways
	}
	// Re-verify calculations one last time to ensure database integrity
	bae.computeTimeframeRanks(bar)
}

// Private helper to isolate the mathematical DNA interval lookup matrix
func (bae *BarAnalyticsEngine) computeTimeframeRanks(bar *models.Bar) {
	token := uint32(bar.InstrumentToken)
	dna, exists := bae.dnaMap[token]
	if !exists || dna == nil || dna.IntervalPercentiles == nil {
		// If baseline DNA is not ready for this stock, default to the neutral median index
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		return
	}

	baseline, hasTimeframeBaseline := dna.IntervalPercentiles[bar.Timeframe]
	if !hasTimeframeBaseline {
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		return
	}

	// Track 1: Live Candlestick Absolute Body Displacement (Net Directional Force)
	candleBody := math.Abs(bar.Close - bar.Open)
	switch {
	case candleBody >= baseline.PriceP97:
		bar.Analytics.PriceRank = 7
	case candleBody >= baseline.PriceP90:
		bar.Analytics.PriceRank = 6
	case candleBody >= baseline.PriceP75:
		bar.Analytics.PriceRank = 5
	case candleBody >= baseline.PriceP50:
		bar.Analytics.PriceRank = 4
	case candleBody >= baseline.PriceP25:
		bar.Analytics.PriceRank = 3
	case candleBody >= baseline.PriceP10:
		bar.Analytics.PriceRank = 2
	default:
		bar.Analytics.PriceRank = 1
	}

	// Track 2: Live Candlestick Total High-to-Low Range (Total Volatility Boundary)
	candleRange := bar.High - bar.Low
	switch {
	case candleRange >= baseline.RangeP97:
		bar.Analytics.RangeRank = 7
	case candleRange >= baseline.RangeP90:
		bar.Analytics.RangeRank = 6
	case candleRange >= baseline.RangeP75:
		bar.Analytics.RangeRank = 5
	case candleRange >= baseline.RangeP50:
		bar.Analytics.RangeRank = 4
	case candleRange >= baseline.RangeP25:
		bar.Analytics.RangeRank = 3
	case candleRange >= baseline.RangeP10:
		bar.Analytics.RangeRank = 2
	default:
		bar.Analytics.RangeRank = 1
	}
}
