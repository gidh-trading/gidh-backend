// Package analytics defines pure mathematical routines and data contracts
// for high-velocity order flow interpretation.
package analytics

import (
	"gidh-backend/internal/service/models"
	"math"
)

// CalculateTapeTelemetry parses a raw transaction lookback window slice and evaluates
// your three primary continuous variables, outputting a hydrated TapeTelemetryUnits struct.
func CalculateTapeTelemetry(ticks []models.TickData) models.TapeTelemetryUnits {
	var metrics models.TapeTelemetryUnits

	if len(ticks) < 2 {
		return metrics
	}

	var netCapitalInflow float64 = 0.0
	var maxPossibleEnergy float64 = 0.0
	var totalExecutedVolume float64 = 0.0

	var highestPrice = ticks[0].LastPrice
	var lowestPrice = ticks[0].LastPrice

	for i := 1; i < len(ticks); i++ {
		prev := ticks[i-1]
		curr := ticks[i]

		priceDelta := curr.LastPrice - prev.LastPrice

		// Extract un-spoofable printed trade message volume
		tickVol := curr.CumulativeVolume - prev.CumulativeVolume
		if tickVol < 0 {
			tickVol = curr.LastTradedQuantity
		}

		floatVol := float64(tickVol)
		totalExecutedVolume += floatVol

		// Tracks extreme limits inside lookback window boundary matrix for spatial efficiency
		if curr.LastPrice > highestPrice {
			highestPrice = curr.LastPrice
		}
		if curr.LastPrice < lowestPrice {
			lowestPrice = curr.LastPrice
		}

		if floatVol == 0.0 || priceDelta == 0.0 {
			continue
		}

		netCapitalInflow += floatVol * priceDelta
		maxPossibleEnergy += floatVol * math.Abs(priceDelta)
	}

	// 1. Calculate Bounded BiasScore [-1.0, +1.0] where volume units and price components cancel out
	if maxPossibleEnergy > 0.0 {
		metrics.BiasScore = netCapitalInflow / maxPossibleEnergy
	} else {
		metrics.BiasScore = 0.0
	}

	// 2. Calculate Absolute Volume-Weighted Price Delta (VwpDelta)
	metrics.VwpDelta = netCapitalInflow

	// 3. Calculate Efficiency (Spatial price progress achieved per unit of volume spent)
	if totalExecutedVolume > 0.0 {
		priceSpan := highestPrice - lowestPrice
		metrics.Efficiency = priceSpan / totalExecutedVolume
	} else {
		metrics.Efficiency = 0.0
	}

	return metrics
}

// CalculateHybridTelemetry processes order flow dynamics smoothly by combining
// live session ticks with historical Market DNA parameters to eliminate cold boots.
func CalculateHybridTelemetry(sample []models.TickData, lookbackTicks int, dna *models.MarketDNA) models.TapeTelemetryUnits {
	k := len(sample)
	if k == 0 {
		return models.TapeTelemetryUnits{BiasScore: 0.0, VwpDelta: 0.0, Efficiency: 0.0}
	}

	var cumulativeVwpDelta float64 = 0
	var totalAbsoluteEnergy float64 = 0
	var totalExecutedVolume int64 = 0

	highestPrice := sample[0].LastPrice
	lowestPrice := sample[0].LastPrice

	// Loop through available live streaming tick slices
	for i := 1; i < k; i++ {
		prev := sample[i-1]
		curr := sample[i]

		priceDelta := curr.LastPrice - prev.LastPrice
		tickVol := curr.CumulativeVolume - prev.CumulativeVolume
		if tickVol < 0 {
			tickVol = curr.LastTradedQuantity
		}

		cumulativeVwpDelta += float64(tickVol) * priceDelta
		totalAbsoluteEnergy += float64(tickVol) * math.Abs(priceDelta)
		totalExecutedVolume += tickVol

		if curr.LastPrice > highestPrice {
			highestPrice = curr.LastPrice
		}
		if curr.LastPrice < lowestPrice {
			lowestPrice = curr.LastPrice
		}
	}

	// Calculate baseline parameters from historical Market DNA profiles
	var dnaVolumeFloor float64 = 1000.0 // Dynamic fallback limit
	if dna != nil {
		// Look for standard deviation across the day's time buckets as a baseline variance proxy
		if len(dna.TimeBuckets) > 0 {
			// Pull the standard deviation from the first available slot as an anchor boundary
			dnaVolumeFloor = dna.TimeBuckets[0].VolumeStd
		}
	}

	// 🧠 THE HYBRID SCALING BRIDGE: Anchor the lookback dynamically using DNA volatility floors
	denominator := totalAbsoluteEnergy
	if k < lookbackTicks {
		remainingTicks := float64(lookbackTicks - k)
		// Assume a baseline minimum price step increment (e.g., 0.05 ticks)
		denominator += remainingTicks * dnaVolumeFloor * 0.05
	}

	biasScore := 0.0
	if denominator > 0 {
		biasScore = cumulativeVwpDelta / denominator
		// Enforce mathematical bounds limits
		if biasScore > 1.0 {
			biasScore = 1.0
		} else if biasScore < -1.0 {
			biasScore = -1.0
		}
	}

	priceSpan := highestPrice - lowestPrice
	efficiency := 0.0
	if totalExecutedVolume > 0 {
		efficiency = priceSpan / float64(totalExecutedVolume)
	}

	return models.TapeTelemetryUnits{
		BiasScore:  biasScore,
		VwpDelta:   cumulativeVwpDelta,
		Efficiency: efficiency,
	}
}
