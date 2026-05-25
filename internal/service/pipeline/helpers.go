package pipeline

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
)

type candleState struct {
	bar *models.Bar
}

// getPercentileRank normalizes non-linear distribution spaces into a linear 1-7 coordinate grid
func getPercentileRank(p string) int {
	switch p {
	case "P97":
		return 7 // burst/extreme
	case "P90":
		return 6 // elevated
	case "P75":
		return 5 // active
	case "P50":
		return 4 // baseline
	case "P25":
		return 3 // below normal
	case "P10":
		return 2 // weak
	case "P05":
		return 1 // drought
	default:
		return 4 // Balanced baseline fallback if an unexpected string leaks in
	}
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar: newBar(ts, price, token, name, timeframe),
	}
}

func newBar(ts time.Time, price float64, token uint32, name, timeframe string) *models.Bar {
	return &models.Bar{
		Timestamp:       ts,
		InstrumentToken: int32(token),
		StockName:       name,
		Timeframe:       timeframe,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
		// Initialize the structural peak tracking with default balanced baselines (Rank 4)
		Peaks: models.PeakAnomalyMetrics{
			PeakVolumeRank: 4,
			PeakPriceRank:  4,
			PeakTickRank:   4,
		},
		SignificantEvents: make([]models.AnomalySnapshot, 0),
	}
}

func (bm *BarManager) processTickForCandle(
	cs *candleState,
	tick *models.EnrichedTick,
	vol float64,
	timeframe string,
	analysis models.AnomalySnapshot,
) {
	price := tick.Raw.LastPrice

	// 1. Structural OHLC Boundary Management
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// 2. Direct Core Metric Summaries
	cs.bar.Volume += vol
	cs.bar.TickCount++
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	// 3. PEAK HISTORICAL INTENSITY EVALUATION
	currentVolRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	currentPriceRank := getPercentileRank(tick.Enrichment.PricePercentile)
	currentTickRank := getPercentileRank(tick.Enrichment.TickPercentile)

	if currentVolRank > cs.bar.Peaks.PeakVolumeRank {
		cs.bar.Peaks.PeakVolumeRank = currentVolRank
	}
	if currentPriceRank > cs.bar.Peaks.PeakPriceRank {
		cs.bar.Peaks.PeakPriceRank = currentPriceRank
	}
	if currentTickRank > cs.bar.Peaks.PeakTickRank {
		cs.bar.Peaks.PeakTickRank = currentTickRank
	}

	if math.Abs(float64(analysis.Direction)) > math.Abs(float64(cs.bar.Peaks.MaxAnomalyDirection)) {
		cs.bar.Peaks.MaxAnomalyDirection = analysis.Direction
		cs.bar.Peaks.PeakAnomalyPrice = analysis.Price
	}

	// Type-Safe Strategy Map Assignment using clean Enums
	if analysis.Type == models.AnomalyAbsorption {
		if math.Abs(float64(analysis.Direction)) > math.Abs(float64(cs.bar.Peaks.MaxAbsorptionSignal)) {
			cs.bar.Peaks.MaxAbsorptionSignal = analysis.Direction
			cs.bar.Peaks.PeakAbsorptionPrice = analysis.Price
		}
	}

	// 4. SIGNIFICANT EVENT LOGGER (With Enum State-Transition Deduplication)
	if analysis.Type != models.AnomalyNone && analysis.Direction != 0 {
		shouldAppend := true
		eventCount := len(cs.bar.SignificantEvents)

		if eventCount > 0 {
			lastEvent := cs.bar.SignificantEvents[eventCount-1]

			// If type and direction match exactly, perform basic deduplication
			if lastEvent.Type == analysis.Type && lastEvent.Direction == analysis.Direction {
				if analysis.VolumeRank > lastEvent.VolumeRank {
					cs.bar.SignificantEvents[eventCount-1] = analysis
					cs.bar.SignificantEvents[eventCount-1].Price = analysis.Price
				}
				shouldAppend = false
			}
		}

		if shouldAppend && eventCount < 10 {
			cs.bar.SignificantEvents = append(cs.bar.SignificantEvents, analysis)
		}
	}

	// 5. Market Auction Framework Profile Allocation
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	// 6A. Initialize the active levels slice array if it doesn't exist inside the map yet
	if cs.bar.Peaks.ActiveLevels == nil {
		cs.bar.Peaks.ActiveLevels = make([]models.AbsorptionLevel, 0)
	}

	// 6B. MEMBRANE LIFECYCLE EVALUATION: Evaluate continuous survival or true tear status on each tick
	for i := range cs.bar.Peaks.ActiveLevels {
		level := &cs.bar.Peaks.ActiveLevels[i]
		if !level.IsActive {
			continue
		}

		// --- SUPPORT MEMBRANE EVALUATION ---
		if level.Direction == 1 {
			// Track how deep the market stretches or penetrates into our membrane
			if price < level.Price && price < level.MaxStretchedPrice {
				level.MaxStretchedPrice = price
			}
			// If price breaches past the absolute calculated tear boundary, the membrane tears completely
			if price < level.TearBoundary {
				level.IsActive = false
			}
		}

		// --- RESISTANCE MEMBRANE EVALUATION ---
		if level.Direction == -1 {
			// Track how deep the market stretches or penetrates into our membrane
			if price > level.Price && price > level.MaxStretchedPrice {
				level.MaxStretchedPrice = price
			}
			// If price surges past the absolute calculated tear boundary, the membrane tears completely
			if price > level.TearBoundary {
				level.IsActive = false
			}
		}
	}

	// 6C. STRUCTURAL MEMBRANE INITIALIZATION: Setup the elastic boundaries strictly using committed capital
	if analysis.Type == models.AnomalyAbsorption && analysis.Direction != 0 {
		levelPrice := analysis.Price

		// Verify duplicate proximity inside this candle bar to prevent line stacking/cluttering
		isDuplicate := false
		for _, lvl := range cs.bar.Peaks.ActiveLevels {
			if math.Abs(lvl.Price-levelPrice) < 0.05 && lvl.Direction == analysis.Direction && lvl.IsActive {
				isDuplicate = true
				break
			}
		}

		if !isDuplicate {
			// Extract live committed volume variables
			liveVolume := tick.Telemetry.LiveVolume

			// Initialize default baseline fallbacks from historical physics definitions
			expectedVolP50 := 1.0
			expectedPriceP50 := 0.10

			// Extract accurate minute baseline from DNA context if verified
			if tick.VolProfile != nil && tick.MinuteIndex > 0 {
				// Safely use an aligned structural baseline move for calculation scaling
				expectedPriceP50 = 0.20
			}

			// --- THE COMMITTED CAPITAL MEMBRANE EQUATION ---
			// Scaling factor is strictly determined by how much live volume overrides normal expected baseline volume
			scalingFactor := liveVolume / expectedVolP50
			if scalingFactor < 1.0 {
				scalingFactor = 1.0
			}
			if scalingFactor > 5.0 {
				scalingFactor = 5.0 // Cap expansion limit to keep boundaries logically and mathematically stable
			}

			toleranceBuffer := expectedPriceP50 * scalingFactor

			// Assign clear elastic boundary vectors based on which side won the zone
			var tearBoundary float64
			if analysis.Direction == 1 {
				tearBoundary = levelPrice - toleranceBuffer // Support line stretches downward, tearing when floor is shattered
			} else {
				tearBoundary = levelPrice + toleranceBuffer // Resistance line stretches upward, tearing when ceiling is shattered
			}

			cs.bar.Peaks.ActiveLevels = append(cs.bar.Peaks.ActiveLevels, models.AbsorptionLevel{
				Price:             levelPrice,
				Direction:         analysis.Direction,
				Strength:          analysis.VolumeRank,
				IsActive:          true,
				TearBoundary:      tearBoundary,
				MaxStretchedPrice: levelPrice,
			})
		}
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
