package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

type rollingEntry struct {
	ts    time.Time
	price float64
	vol   float64
}

type RollingState struct {
	queue []rollingEntry

	Volume float64
	Open   float64
	High   float64
	Low    float64
	Close  float64
}

type Bar struct {
	Timestamp       time.Time         `json:"timestamp"`
	InstrumentToken int32             `json:"instrument_token"`
	StockName       string            `json:"stock_name"`
	Timeframe       string            `json:"timeframe"`
	Open            float64           `json:"open"`
	High            float64           `json:"high"`
	Low             float64           `json:"low"`
	Close           float64           `json:"close"`
	Volume          float64           `json:"volume"`
	VWAP            float64           `json:"vwap,omitempty"`
	POC             float64           `json:"poc,omitempty"`
	VAH             float64           `json:"vah,omitempty"`
	VAL             float64           `json:"val,omitempty"`
	VolEnergy       float64           `json:"vol_energy,omitempty"`
	RngEnergy       float64           `json:"rng_energy,omitempty"`
	Ticks           []models.TickData `json:"ticks,omitempty"`
}

type SessionState struct {
	TotalVolume float64
	TotalRange  float64
	Count       int
}
