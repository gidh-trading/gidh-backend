package pipeline

import (
	"math"
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
		VolumeRank:      1,
		TickRank:        1,
		PriceRank:       4, // Default to a standard baseline state
	}
}

func (bm *BarManager) processTickForCandle(
	cs *candleState,
	tick *models.EnrichedTick,
	vol float64,
	timeframe string,
) {
	price := tick.Raw.LastPrice

	// 1. Structural Candlestick Boundary Extensions
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// 2. Accumulate Totals
	cs.bar.Volume += vol
	cs.bar.TickCount++

	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	// 3. Peak Volume and Tick Intensity tracking across interval lifetime
	if tick.Enrichment.VolumeRank > cs.bar.VolumeRank {
		cs.bar.VolumeRank = tick.Enrichment.VolumeRank
	}
	if tick.Enrichment.TickRank > cs.bar.TickRank {
		cs.bar.TickRank = tick.Enrichment.TickRank
	}

	// 4. 🔥 ALIGNED LIVE VOLATILITY & BODY SEPARATION
	if dna, ok := bm.dnaMap[uint32(cs.bar.InstrumentToken)]; ok && dna != nil {
		if baseline, hasTimeframeBaseline := dna.IntervalPercentiles[timeframe]; hasTimeframeBaseline {

			// Track 1: Live Candlestick Absolute Body Displacement (Net Directional Force)
			candleBody := math.Abs(cs.bar.Close - cs.bar.Open)
			switch {
			case candleBody >= baseline.PriceP97:
				cs.bar.PriceRank = 7
			case candleBody >= baseline.PriceP90:
				cs.bar.PriceRank = 6
			case candleBody >= baseline.PriceP75:
				cs.bar.PriceRank = 5
			case candleBody >= baseline.PriceP50:
				cs.bar.PriceRank = 4
			case candleBody >= baseline.PriceP25:
				cs.bar.PriceRank = 3
			case candleBody >= baseline.PriceP10:
				cs.bar.PriceRank = 2
			default:
				cs.bar.PriceRank = 1
			}

			// Track 2: Live Candlestick Total High-to-Low Range (Total Volatility Boundary)
			candleRange := cs.bar.High - cs.bar.Low
			switch {
			case candleRange >= baseline.RangeP97:
				cs.bar.RangeRank = 7
			case candleRange >= baseline.RangeP90:
				cs.bar.RangeRank = 6
			case candleRange >= baseline.RangeP75:
				cs.bar.RangeRank = 5
			case candleRange >= baseline.RangeP50:
				cs.bar.RangeRank = 4
			case candleRange >= baseline.RangeP25:
				cs.bar.RangeRank = 3
			case candleRange >= baseline.RangeP10:
				cs.bar.RangeRank = 2
			default:
				cs.bar.RangeRank = 1
			}
		}
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
