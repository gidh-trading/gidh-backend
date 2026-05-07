package pipeline

import (
	"time"
)

type rollingEntry struct {
	ts    time.Time
	price float64
	vol   float64
}

type RollingState struct {
	queue   []rollingEntry
	Volume  float64
	Open    float64
	High    float64
	Low     float64
	Close   float64
	LastDir int
}

type SessionState struct {
	TotalVolume float64
	TotalRange  float64
	Count       int
}
