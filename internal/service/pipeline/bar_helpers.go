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

	rawEfficiency := 0.0
	if candleRange > 0 {
		rawEfficiency = candleBody / candleRange
	}

	switch {
	case rawEfficiency >= 0.95:
		bar.Analytics.EfficiencyRank = 7
	case rawEfficiency >= 0.80:
		bar.Analytics.EfficiencyRank = 6
	case rawEfficiency >= 0.65:
		bar.Analytics.EfficiencyRank = 5
	case rawEfficiency >= 0.45:
		bar.Analytics.EfficiencyRank = 4
	case rawEfficiency >= 0.30:
		bar.Analytics.EfficiencyRank = 3
	case rawEfficiency >= 0.15:
		bar.Analytics.EfficiencyRank = 2
	default:
		bar.Analytics.EfficiencyRank = 1
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

	candleBodyTop := math.Max(bar.Open, bar.Close)
	candleBodyBottom := math.Min(bar.Open, bar.Close)

	upperWick := bar.High - candleBodyTop
	lowerWick := candleBodyBottom - bar.Low

	upperWickRatio := upperWick / candleRange
	lowerWickRatio := lowerWick / candleRange

	positionRatio := (bar.Close - bar.Low) / candleRange
	isHigherThanOpen := bar.Close > bar.Open
	isLowerThanOpen := bar.Close < bar.Open

	bar.Analytics.UpperWickRank = bae.getWickRank(upperWickRatio)
	bar.Analytics.LowerWickRank = bae.getWickRank(lowerWickRatio)

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
