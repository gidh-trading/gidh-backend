package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

type macroPoint struct {
	x      float64
	price  float64
	vwap   float64
	volume float64
}

type candleState struct {
	bar        *models.Bar
	macroQueue []macroPoint
}

// getPercentileRank normalizes non-linear distribution spaces into a linear 1-7 coordinate grid
func getPercentileRank(p string) int {
	switch p {
	case "P99":
		return 7
	case "P95":
		return 6
	case "P90":
		return 5
	case "P50":
		return 4
	case "NORMAL":
		return 3
	case "P10":
		return 2
	case "P05":
		return 1
	default:
		return 3 // Default fallback to mid-tier stationary state
	}
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar:        newBar(ts, price, token, name, timeframe),
		macroQueue: make([]macroPoint, 0, 10),
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
		Ticks:           make([]models.TickData, 0),
	}
}

func (bm *BarManager) processTickForCandle(cs *candleState, tick *models.EnrichedTick, vol float64, timeframe string) {
	price := tick.Raw.LastPrice

	// 1. Structural OHLC Boundary Management
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// 2. Direct Aggregate Summaries
	cs.bar.Volume += vol
	cs.bar.TickCount++
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	// 3. CONTINUOUS LIVE TELEMETRY DEFAULT INITIALIZATION
	if cs.bar.Metrics.PeakRelativeVolumeRank == 0 {
		cs.bar.Metrics.PeakRelativeVolumeRank = 3
	}
	if cs.bar.Metrics.PeakRangeRank == 0 {
		cs.bar.Metrics.PeakRangeRank = 3
	}
	if cs.bar.Metrics.PeakTickRank == 0 {
		cs.bar.Metrics.PeakTickRank = 3
	}

	// ------------------------------------------------------------------------
	// VISUALIZATION LAYER SYMMETRIC COMPRESSION ENGINE
	// ------------------------------------------------------------------------

	// A. Capture Peak Participation Anomaly Envelope (Horizontal Heatmap Grid)
	currentVolRank := getPercentileRank(tick.Enrichment.RelativeVolumePercentile)
	if cs.bar.Metrics.PeakRelativeVolumeRank == 3 {
		cs.bar.Metrics.PeakRelativeVolumeRank = currentVolRank
	} else {
		// Prioritize extreme extensions (Expansion or total drought anomalies overrule NORMAL/P50)
		if currentVolRank > 4 && currentVolRank > cs.bar.Metrics.PeakRelativeVolumeRank {
			cs.bar.Metrics.PeakRelativeVolumeRank = currentVolRank
		} else if currentVolRank < 3 && currentVolRank < cs.bar.Metrics.PeakRelativeVolumeRank {
			cs.bar.Metrics.PeakRelativeVolumeRank = currentVolRank
		}
	}

	// B. Capture Peak Response Anomaly Envelope (Vertical Heatmap Grid)
	currentRangeRank := getPercentileRank(tick.Enrichment.RangePercentile)
	if cs.bar.Metrics.PeakRangeRank == 3 {
		cs.bar.Metrics.PeakRangeRank = currentRangeRank
	} else {
		if currentRangeRank > 4 && currentRangeRank > cs.bar.Metrics.PeakRangeRank {
			cs.bar.Metrics.PeakRangeRank = currentRangeRank
		} else if currentRangeRank < 3 && currentRangeRank < cs.bar.Metrics.PeakRangeRank {
			cs.bar.Metrics.PeakRangeRank = currentRangeRank
		}
	}

	// C. Capture Peak Tick Anomaly Envelope (Velocity Heatmap Grid)
	currentTickRank := getPercentileRank(tick.Enrichment.TickPercentile)
	if cs.bar.Metrics.PeakTickRank == 3 {
		cs.bar.Metrics.PeakTickRank = currentTickRank
	} else {
		if currentTickRank > 4 && currentTickRank > cs.bar.Metrics.PeakTickRank {
			cs.bar.Metrics.PeakTickRank = currentTickRank
		} else if currentTickRank < 3 && currentTickRank < cs.bar.Metrics.PeakTickRank {
			cs.bar.Metrics.PeakTickRank = currentTickRank
		}
	}

	// 5. Build Market Auction Framework Layout
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.state1m = make(map[uint32]*candleState)
	bm.state3m = make(map[uint32]*candleState)
	bm.state5m = make(map[uint32]*candleState)
	bm.state10m = make(map[uint32]*candleState)
	bm.state15m = make(map[uint32]*candleState)
}
