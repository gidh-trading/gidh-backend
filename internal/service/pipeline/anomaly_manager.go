package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

type AnomalyManager struct {
	MinImbalancePct   float64
	MinIntensityFloor float64
}

func NewAnomalyManager() *AnomalyManager {
	return &AnomalyManager{
		MinImbalancePct:   0.15,
		MinIntensityFloor: 50.0,
	}
}

// GetDominantAnomaly parses all footprint cells across a candle timeframe to choose exactly one winner
func (am *AnomalyManager) GetDominantAnomaly(rawCells map[float64]*models.HeatmapCell) models.UIDominantAnomaly {
	var winner *models.HeatmapCell
	var maxIntensity float64 = -1.0

	// 🌟 FIX 1: Define a logical baseline intensity floor now that it's normalized
	const MinNormalizedIntensityFloor = 3.0

	for _, cell := range rawCells {
		transactedVol := cell.AggressiveBuy + cell.AggressiveSell
		if transactedVol == 0 {
			continue
		}

		tradeDelta := cell.AggressiveBuy - cell.AggressiveSell
		imbalancePct := math.Abs(tradeDelta) / transactedVol

		if imbalancePct < am.MinImbalancePct {
			continue
		}

		// 🌟 FIX 2: Normalize the massive raw intensity score by total volume
		normalizedIntensity := cell.IntensityScore / transactedVol

		// Filter out weak baseline jitter
		if normalizedIntensity < MinNormalizedIntensityFloor {
			continue
		}

		if normalizedIntensity > maxIntensity {
			maxIntensity = normalizedIntensity
			winner = cell
		}
	}

	if winner == nil {
		return models.UIDominantAnomaly{IsPresent: false}
	}

	anomalyType := "WHALE"
	if winner.MaxTickZ > winner.MaxVolumeZ {
		anomalyType = "ICEBERG"
	}

	// 🌟 FIX 3: Return the clean normalized intensity to your UI/Analytics
	return models.UIDominantAnomaly{
		IsPresent: true,
		Type:      anomalyType,
		P:         winner.PriceBin,
		V:         winner.CellVolume,
		D:         winner.AggressiveBuy - winner.AggressiveSell,
		I:         maxIntensity,
	}
}
