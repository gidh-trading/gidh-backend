package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

func newBar(ts time.Time, price float64, token uint32, name string, timeframe string) *models.Bar {
	var truncatedTs time.Time
	switch timeframe {
	case "5m":
		truncatedTs = ts.Truncate(5 * time.Minute)
	case "3m":
		truncatedTs = ts.Truncate(3 * time.Minute)
	default:
		truncatedTs = ts.Truncate(time.Minute)
	}

	return &models.Bar{
		Timestamp:       truncatedTs,
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
