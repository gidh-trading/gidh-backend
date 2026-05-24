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
	if analysis.Type == models.AnomalyAbsorption { // ◄ Compile-safe comparison
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

			// If type and direction match exactly, we perform basic deduplication
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

	// 6A. Initialize the levels slice array if it doesn't exist inside the map yet
	if cs.bar.Peaks.ActiveLevels == nil {
		cs.bar.Peaks.ActiveLevels = make([]models.AbsorptionLevel, 0)
	}

	// 6B. BREAK TEST: Evaluate existing active lines against the new price tick
	for i := range cs.bar.Peaks.ActiveLevels {
		level := &cs.bar.Peaks.ActiveLevels[i]
		if !level.IsActive {
			continue
		}

		// If a support floor is penetrated by a price breaking down, deactivate it
		if level.Direction == 1 && price < level.Price {
			level.IsActive = false
		}
		// If a resistance ceiling is penetrated by a price breaking up, deactivate it
		if level.Direction == -1 && price > level.Price {
			level.IsActive = false
		}
	}

	// 6C. CREATE NEW LEVEL: If the current tick is a fresh absorption signal, register a new line
	if analysis.Type == models.AnomalyAbsorption && analysis.Direction != 0 {
		levelPrice := cs.bar.Low
		if analysis.Direction == -1 {
			levelPrice = cs.bar.High // Resistance forms at the absolute top wick edge
		}

		// Verify if we already registered an identical price point line inside this candle bar to avoid clutter
		isDuplicate := false
		for _, lvl := range cs.bar.Peaks.ActiveLevels {
			if lvl.Price == levelPrice && lvl.Direction == analysis.Direction && lvl.IsActive {
				isDuplicate = true
				break
			}
		}

		if !isDuplicate {
			cs.bar.Peaks.ActiveLevels = append(cs.bar.Peaks.ActiveLevels, models.AbsorptionLevel{
				Price:     levelPrice,
				Direction: analysis.Direction,
				Strength:  analysis.VolumeRank,
				IsActive:  true,
			})
		}
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
