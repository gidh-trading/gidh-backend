package pipeline

import "time"

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
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	VolEnergy float64
	RngEnergy float64
	Start     time.Time
}

type SessionState struct {
	TotalVolume float64
	TotalRange  float64
	Count       int
}
