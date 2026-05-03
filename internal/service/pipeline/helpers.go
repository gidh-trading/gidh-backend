package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

func newBar(ts time.Time, price float64, token uint32, name string, timeframe string) *models.Bar {
	// Truncate based on timeframe
	var truncatedTs time.Time
	if timeframe == "5m" {
		// Truncate to 5-minute interval
		truncatedTs = ts.Truncate(5 * time.Minute)
	} else {
		// Default to 1-minute truncation
		truncatedTs = ts.Truncate(time.Minute)
	}

	return &models.Bar{
		Timestamp:       truncatedTs, // Now starts at :00
		InstrumentToken: int32(token),
		StockName:       name,
		Timeframe:       timeframe,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
		Volume:          0,
		Ticks:           make([]models.TickData, 0, 60),
	}
}

func updateBar(b *models.Bar, price, vol float64) {
	if price > b.High {
		b.High = price
	}
	if price < b.Low {
		b.Low = price
	}
	b.Close = price
	b.Volume += vol
}
