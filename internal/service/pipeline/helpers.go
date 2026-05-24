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
	}

	// Type-Safe Strategy Map Assignment using clean Enums
	if analysis.Type == models.AnomalyAbsorption { // ◄ Compile-safe comparison
		if math.Abs(float64(analysis.Direction)) > math.Abs(float64(cs.bar.Peaks.MaxAbsorptionSignal)) {
			cs.bar.Peaks.MaxAbsorptionSignal = analysis.Direction
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
					cs.bar.SignificantEvents[eventCount-1] = analysis // Inline update escalation
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
	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
