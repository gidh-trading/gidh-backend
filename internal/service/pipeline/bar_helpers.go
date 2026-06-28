package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
)

func (bae *BarAnalyticsEngine) computeMacroTimeframeRanksAndDirection(bar *models.Bar) {
	token := uint32(bar.InstrumentToken)
	dna, exists := bae.dnaMap[token]
	if !exists || dna == nil || dna.IntervalPercentiles == nil {
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		bar.Analytics.Direction = models.DirSideways
		return
	}

	baseline, hasTimeframeBaseline := dna.IntervalPercentiles[bar.Timeframe]
	if !hasTimeframeBaseline {
		bar.Analytics.PriceRank = 4
		bar.Analytics.RangeRank = 4
		bar.Analytics.Direction = models.DirSideways
		return
	}

	candleBody := math.Abs(bar.Close - bar.Open)
	candleRange := bar.High - bar.Low

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

	if candleRange <= 0 {
		bar.Analytics.Direction = models.DirSideways
		return
	}

	positionRatio := (bar.Close - bar.Low) / candleRange
	isHigherThanOpen := bar.Close > bar.Open
	isLowerThanOpen := bar.Close < bar.Open

	if bar.Analytics.VolumeRank >= 6 && bar.Analytics.PriceRank <= 4 {
		if positionRatio >= 0.50 {
			bar.Analytics.Direction = models.DirBullishAbsorption
			return
		} else {
			bar.Analytics.Direction = models.DirBearishAbsorption
			return
		}
	}

	switch {
	case positionRatio >= 0.85 && isHigherThanOpen:
		bar.Analytics.Direction = models.DirStrongBullish
	case positionRatio > 0.55 && isHigherThanOpen:
		bar.Analytics.Direction = models.DirBullish
	case positionRatio <= 0.15 && isLowerThanOpen:
		bar.Analytics.Direction = models.DirStrongBearish
	case positionRatio < 0.45 && isLowerThanOpen:
		bar.Analytics.Direction = models.DirBearish
	default:
		bar.Analytics.Direction = models.DirSideways
	}
}

func (bae *BarAnalyticsEngine) calculateDistance(price, vwap float64, token uint32) float64 {
	if vwap <= 0 {
		return 0.0
	}
	rawPct := ((price - vwap) / vwap) * 100.0
	if profile, ok := bae.profiles[token]; ok && profile != nil && profile.ADRPct > 0 {
		return rawPct / profile.ADRPct
	}
	return rawPct
}

func (bae *BarAnalyticsEngine) getWickRank(ratio float64) int {
	switch {
	case ratio >= 0.45:
		return 7
	case ratio >= 0.35:
		return 6
	case ratio >= 0.25:
		return 5
	case ratio >= 0.18:
		return 4
	case ratio >= 0.12:
		return 3
	case ratio >= 0.05:
		return 2
	default:
		return 1
	}
}

func (bae *BarAnalyticsEngine) computeAnchorRank(anchor *TrackedAnchor, currentPrice, currentVolume, normalizationFactor float64) int {
	if !anchor.IsActive {
		return 0 // Neutral / Not triggered yet
	}

	// Dynamic calculation containing temporary uncommitted intra-bar context
	tempPV := anchor.CumPV + (currentPrice * currentVolume)
	tempVol := anchor.CumVolume + currentVolume

	if tempVol <= 0 {
		return 0
	}

	avwap := tempPV / tempVol
	rawDivergence := currentPrice - avwap

	return bae.getSignedHeatmapRank(rawDivergence, normalizationFactor)
}

// getSignedHeatmapRank maps divergence against an expected benchmark value into a strict -7 to 7 score matrix
func (bae *BarAnalyticsEngine) getSignedHeatmapRank(divergence, benchmark float64) int {
	if benchmark <= 0 || math.Abs(divergence) < 0.00001 {
		return 0 // Absolute Neutral
	}

	// Quantization boundaries as percentages of the benchmark target
	// Sliced symmetrically to distribute steps evenly on your heatmap
	thresholds := [6]float64{0.05, 0.15, 0.30, 0.50, 0.75, 1.10}
	absDiv := math.Abs(divergence)

	var magnitude int
	switch {
	case absDiv >= benchmark*thresholds[5]:
		magnitude = 7
	case absDiv >= benchmark*thresholds[4]:
		magnitude = 6
	case absDiv >= benchmark*thresholds[3]:
		magnitude = 5
	case absDiv >= benchmark*thresholds[2]:
		magnitude = 4
	case absDiv >= benchmark*thresholds[1]:
		magnitude = 3
	case absDiv >= benchmark*thresholds[0]:
		magnitude = 2
	default:
		magnitude = 1
	}

	// Apply dynamic polarity alignment
	if divergence < 0 {
		return -magnitude
	}
	return magnitude
}

// EvaluateAndLockAnchors processes structural threshold conditions at bar completion boundaries to activate anchors
func (bae *BarAnalyticsEngine) EvaluateAndLockAnchors(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	// 1. Structural Price Band Breaches
	if bar.High >= bar.Analytics.ADRHigh && !h.AnchorADRHigh.IsActive {
		h.AnchorADRHigh = TrackedAnchor{IsActive: true}
	}
	if bar.Low <= bar.Analytics.ADRLow && !h.AnchorADRLow.IsActive {
		h.AnchorADRLow = TrackedAnchor{IsActive: true}
	}

	// 2. Pure Symmetrical Raw Percentage Deviations from VWAP (0.5% threshold)
	var rawVwapDistPct float64 = 0.0
	if bar.VWAP > 0 {
		rawVwapDistPct = ((bar.Close - bar.VWAP) / bar.VWAP) * 100.0
	}

	if rawVwapDistPct >= 0.5 && !h.AnchorDistGt.IsActive {
		h.AnchorDistGt = TrackedAnchor{IsActive: true}
	}
	if rawVwapDistPct <= -0.5 && !h.AnchorDistLt.IsActive {
		h.AnchorDistLt = TrackedAnchor{IsActive: true}
	}
}

// AccumulateAnchorContext appends historical volume weighted context variables safely upon bar confirmation
func (bae *BarAnalyticsEngine) AccumulateAnchorContext(bar *models.Bar, h *TimeframeAnalyticsHistory) {
	if h.AnchorADRHigh.IsActive {
		h.AnchorADRHigh.CumPV += bar.Close * bar.Volume
		h.AnchorADRHigh.CumVolume += bar.Volume
	}
	if h.AnchorADRLow.IsActive {
		h.AnchorADRLow.CumPV += bar.Close * bar.Volume
		h.AnchorADRLow.CumVolume += bar.Volume
	}
	if h.AnchorDistGt.IsActive {
		h.AnchorDistGt.CumPV += bar.Close * bar.Volume
		h.AnchorDistGt.CumVolume += bar.Volume
	}
	if h.AnchorDistLt.IsActive {
		h.AnchorDistLt.CumPV += bar.Close * bar.Volume
		h.AnchorDistLt.CumVolume += bar.Volume
	}
}
