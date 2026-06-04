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
