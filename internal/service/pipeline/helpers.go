package pipeline

import "time"

func newBar(ts time.Time, price float64) *Bar {
	return &Bar{
		Open:      price,
		High:      price,
		Low:       price,
		Close:     price,
		Timestamp: ts,
		VolEnergy: 0,
		RngEnergy: 0,
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
