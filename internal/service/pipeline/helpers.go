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
		VolumeRank:      1, // Absolute initial floor baseline coordinates
		TickRank:        1,
		PriceRank:       1,
	}
}

// processTickForCandle aggregates tick details into standard OHLC structures.
// It acts as a passive carrier executing peak tracking signatures.
func (bm *BarManager) processTickForCandle(
	cs *candleState,
	tick *models.EnrichedTick,
	vol float64,
	timeframe string,
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

	// 4. 🔥 REFACTORED SEPARATION OF CAPTURE ENGINES

	// A. Participation Ranks track maximum intensity reached during life
	if tick.Enrichment.VolumeRank > cs.bar.VolumeRank {
		cs.bar.VolumeRank = tick.Enrichment.VolumeRank
	}

	if tick.Enrichment.TickRank > cs.bar.TickRank {
		cs.bar.TickRank = tick.Enrichment.TickRank
	}

	// 5. 🔥 DECOUPLED INDEPENDENT BAR TIME-SERIES VOLATILITY ESTIMATION
	// Pull the stock's profile from the mapped manager store
	if prof, ok := bm.profiles[tick.Raw.InstrumentToken]; ok && prof != nil && prof.ATR14 > 0 {
		barRange := cs.bar.High - cs.bar.Low
		volatilityFactor := barRange / prof.ATR14

		// Scale threshold steps contextually normalized against the total bar length
		switch {
		case volatilityFactor >= 0.20:
			cs.bar.PriceRank = 7 // Saturated Volatility Expansion
		case volatilityFactor >= 0.10:
			cs.bar.PriceRank = 6
		case volatilityFactor >= 0.05:
			cs.bar.PriceRank = 5
		case volatilityFactor >= 0.02:
			cs.bar.PriceRank = 4 // Baseline standard drift normal boundary
		case volatilityFactor >= 0.01:
			cs.bar.PriceRank = 3 // Structural limits compression box
		case volatilityFactor >= 0.005:
			cs.bar.PriceRank = 2 // Significant High-Volume Absorption Sign
		default:
			cs.bar.PriceRank = 1 // Ultimate Order-Book Limit Wall Deadlock
		}
	} else {
		// Fallback baseline if profile row doesn't exist
		cs.bar.PriceRank = tick.Enrichment.PriceRank
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}
