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

	// 4. 🔥 ALIGNED MANUAL VOLATILITY SEPARATION (Using Candle Body Displacement)
	if prof, ok := bm.profiles[tick.Raw.InstrumentToken]; ok && prof != nil && prof.ATR14 > 0 {
		// Calculate the absolute directional body move (matches what your UI displays)
		candleBodyMove := math.Abs(cs.bar.Close - cs.bar.Open)
		volatilityFactor := candleBodyMove / prof.ATR14

		switch {
		case volatilityFactor >= 0.20:
			cs.bar.PriceRank = 7 // Saturated Volatility Expansion (P97 Magenta)
		case volatilityFactor >= 0.10:
			cs.bar.PriceRank = 6 // Significant Outlier Velocity (P90 Purple)
		case volatilityFactor >= 0.05:
			cs.bar.PriceRank = 5 // Active Directional Flow Expansion (P75 Orange)
		case volatilityFactor >= 0.02:
			cs.bar.PriceRank = 4 // Baseline standard drift normal boundary (P50 Yellow)
		case volatilityFactor >= 0.01:
			cs.bar.PriceRank = 3 // Structural limits compression box
		case volatilityFactor >= 0.005:
			cs.bar.PriceRank = 2 // High-Volume Absorption Sign
		default:
			cs.bar.PriceRank = 1 // Absolute Pricing Deadlock
		}
	} else {
		if tick.Enrichment.PriceRank > cs.bar.PriceRank {
			cs.bar.PriceRank = tick.Enrichment.PriceRank
		}
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
