// internal/service/pipeline/analytics.go

package pipeline

import (
	"math"

	"gidh-backend/internal/service/models"
)

type AnalyticsStage struct{}

func NewAnalyticsStage() *AnalyticsStage {
	return &AnalyticsStage{}
}

func (s *AnalyticsStage) Process(tick *models.EnrichedTick) error {
	// 1. Evaluate Anomaly Threshold Rules based on the stats provided by Enrichment
	isAnomaly := false
	if tick.HasBaseline {
		if tick.VolumeZ > 2.0 {
			isAnomaly = true
		}
	} else {
		if tick.LiveTickCount >= 100 {
			isAnomaly = true
		}
	}

	// If it doesn't qualify as a high-volume anomaly, exit early
	if !isAnomaly || len(tick.WindowTicks) == 0 {
		return nil
	}

	// 2. Handle Geometric Grid Snapping
	price := tick.Raw.LastPrice
	bucketSize := 1.0

	if tick.FullVolProfile != nil && tick.FullVolProfile.BucketSize > 0 {
		bucketSize = tick.FullVolProfile.BucketSize
	} else if price > 0 {
		if price > 5000 {
			bucketSize = 5.0
		} else if price > 1000 {
			bucketSize = 1.0
		} else {
			bucketSize = 0.5
		}
	}

	binVolumes := make(map[float64]float64)
	binCounts := make(map[float64]int)
	maxBinVolume := 0.0

	// Aggregate the lookback ticks into clean horizontal price compartments
	for _, t := range tick.WindowTicks {
		snappedPrice := math.Floor(t.Price/bucketSize) * bucketSize
		binVolumes[snappedPrice] += t.Volume
		binCounts[snappedPrice]++
		if binVolumes[snappedPrice] > maxBinVolume {
			maxBinVolume = binVolumes[snappedPrice]
		}
	}

	// 3. Compute relative glow values for the UI layer
	var cells []models.HeatmapCell
	if maxBinVolume > 0 {
		for priceBin, volumeSum := range binVolumes {
			intensityRatio := volumeSum / maxBinVolume
			cells = append(cells, models.HeatmapCell{
				PriceBin:       priceBin,
				AnomalyCount:   binCounts[priceBin],
				IntensityScore: intensityRatio,
			})
		}
	}

	// Attach the geometric heatmap coordinates to the context payload
	tick.AnomalyCells = cells
	return nil
}
