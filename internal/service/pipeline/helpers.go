package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

func newBar(ts time.Time, price float64, token uint32, name string, timeframe string) *Bar {
	return &Bar{
		Timestamp:       ts,
		InstrumentToken: int32(token),
		StockName:       name,
		Timeframe:       timeframe,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
		VolEnergy:       0,
		RngEnergy:       0,
		Ticks:           make([]models.TickData, 0, 60), // Pre-allocate for the 60-tick burst
	}
}

func updateBar(b *Bar, price, vol float64) {
	if price > b.High {
		b.High = price
	}
	if price < b.Low {
		b.Low = price
	}
	b.Close = price
	b.Volume += vol
}
