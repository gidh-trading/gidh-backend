package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

type AnomalyManager struct {
	MinImbalancePct float64
}

func NewAnomalyManager() *AnomalyManager {
	return &AnomalyManager{
		MinImbalancePct: 0.15, // Ignores noise when neither side possesses an imbalance edge
	}
}

// GetDominantAnomaly parses all footprint cells across a candle timeframe to choose exactly one winner
func (am *AnomalyManager) GetDominantAnomaly(rawCells map[float64]*models.HeatmapCell) models.UIDominantAnomaly {
	var winner *models.HeatmapCell
	var maxIntensity float64 = -1.0

	// 1. Battle Royale: Pick the footprint block with the highest intensity profile
	for _, cell := range rawCells {
		transactedVol := cell.AggressiveBuy + cell.AggressiveSell
		if transactedVol == 0 {
			continue
		}

		tradeDelta := cell.AggressiveBuy - cell.AggressiveSell
		imbalancePct := math.Abs(tradeDelta) / transactedVol

		// Drop low directional intent blocks
		if imbalancePct < am.MinImbalancePct {
			continue
		}

		if cell.IntensityScore > maxIntensity {
			maxIntensity = cell.IntensityScore
			winner = cell
		}
	}

	// 2. Fallback execution path if no anomaly breaks past thresholds
	if winner == nil {
		return models.UIDominantAnomaly{IsPresent: false}
	}

	// 3. Deduce footprint identity archetype: Whale vs Iceberg
	anomalyType := "WHALE"
	if winner.MaxTickZ > winner.MaxVolumeZ {
		anomalyType = "ICEBERG"
	}

	return models.UIDominantAnomaly{
		IsPresent: true,
		Type:      anomalyType,
		P:         winner.PriceBin,
		V:         winner.CellVolume,
		D:         winner.AggressiveBuy - winner.AggressiveSell,
		I:         winner.IntensityScore,
	}
}
