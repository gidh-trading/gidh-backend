package pipeline

import (
	"time"

	"gidh-backend/internal/service/models"
)

type candleState struct {
	bar *models.Bar
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

	// 3. Market Auction Framework Profile Allocation
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}

// classifyPercentile maps the raw value against the DNA baseline percentiles
func classifyPercentile(value, p05, p10, p25, p50, p75, p90, p97 float64) string {
	switch {
	case value >= p97:
		return "P97" // burst/extreme
	case value >= p90:
		return "P90" // elevated
	case value >= p75:
		return "P75" // active
	case value >= p50:
		return "P50" // baseline
	case value >= p25:
		return "P25" // below normal
	case value >= p10:
		return "P10" // weak
	case value >= p05:
		return "P05" // drought
	default:
		return "DROUGHT_EXTREME" // Anything below P05 falls entirely below the grid floor
	}
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
