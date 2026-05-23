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
		cs.bar.Metrics.PeakRelativeVolumeRank = 4
	}
	if cs.bar.Metrics.PeakRangeRank == 0 {
		cs.bar.Metrics.PeakRangeRank = 4
	}
	if cs.bar.Metrics.PeakTickRank == 0 {
		cs.bar.Metrics.PeakTickRank = 4
	}

	// ------------------------------------------------------------------------
	// VISUALIZATION LAYER SYMMETRIC COMPRESSION ENGINE
	// ------------------------------------------------------------------------

	// A1. Volume Z-Score (Absolute Historical Statistical Depth)
	currentVolZRank := getPercentileRank(tick.Enrichment.VolumeZPercentile)
	if currentVolZRank > 4 && currentVolZRank > cs.bar.Metrics.PeakVolumeZRank {
		cs.bar.Metrics.PeakVolumeZRank = currentVolZRank
	} else if currentVolZRank < 4 && currentVolZRank < cs.bar.Metrics.PeakVolumeZRank {
		cs.bar.Metrics.PeakVolumeZRank = currentVolZRank
	}

	// A2. Capture Peak Participation Anomaly Envelope (Horizontal Heatmap Grid)
	currentVolRank := getPercentileRank(tick.Enrichment.RelativeVolumePercentile)
	if currentVolRank > 4 && currentVolRank > cs.bar.Metrics.PeakRelativeVolumeRank {
		cs.bar.Metrics.PeakRelativeVolumeRank = currentVolRank
	} else if currentVolRank < 4 && currentVolRank < cs.bar.Metrics.PeakRelativeVolumeRank {
		cs.bar.Metrics.PeakRelativeVolumeRank = currentVolRank
	}

	// B. Range (Realized Response): Prioritize expansions OR range squeeze over baseline
	currentRangeRank := getPercentileRank(tick.Enrichment.RangePercentile)
	if currentRangeRank > 4 && currentRangeRank > cs.bar.Metrics.PeakRangeRank {
		cs.bar.Metrics.PeakRangeRank = currentRangeRank
	} else if currentRangeRank < 4 && currentRangeRank < cs.bar.Metrics.PeakRangeRank {
		cs.bar.Metrics.PeakRangeRank = currentRangeRank
	}

	// C. Ticks (Execution Tempo): Track highest *sustained velocity* within this bar.
	// Because low ticks aren't a "negative event", we want to capture the highest energy level
	// achieved during the candle's lifetime (e.g., if the market accelerates to a Burst, the bar remembers it).
	currentTickRank := getPercentileRank(tick.Enrichment.TickPercentile)
	if currentTickRank > cs.bar.Metrics.PeakTickRank {
		cs.bar.Metrics.PeakTickRank = currentTickRank
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
