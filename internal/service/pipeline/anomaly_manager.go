package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

type AnomalyManager struct {
	MinImbalancePct        float64
	MinNormalizedIntensity float64
}

func NewAnomalyManager() *AnomalyManager {
	return &AnomalyManager{
		MinImbalancePct:        0.10, // Drop to 10% to catch more balanced fight zones
		MinNormalizedIntensity: 1.5,  // Drop from 3.0 to 1.5 to show moderate high-volume nodes
	}
}

// GetDominantAnomaly parses all footprint cells across a candle timeframe to choose exactly one winner
func (am *AnomalyManager) GetDominantAnomaly(rawCells map[float64]*models.HeatmapCell) models.UIDominantAnomaly {
	var winner *models.HeatmapCell
	var maxIntensity float64 = -1.0

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

		// Normalize the intensity score by total volume
		normalizedIntensity := cell.IntensityScore / transactedVol

		// 👈 Use the struct's configurable property to filter weak baseline jitter
		if normalizedIntensity < am.MinNormalizedIntensity {
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

	// Return the clean normalized intensity to your UI/Analytics
	return models.UIDominantAnomaly{
		IsPresent: true,
		Type:      anomalyType,
		P:         winner.PriceBin,
		V:         winner.CellVolume,
		D:         winner.AggressiveBuy - winner.AggressiveSell,
		I:         maxIntensity,
	}
}
