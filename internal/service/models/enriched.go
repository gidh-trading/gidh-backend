package models

import "time"

type DirectionState string

const (
	DirStrongBullish DirectionState = "STRONG_BULLISH"
	DirBullish       DirectionState = "BULLISH"
	DirSideways      DirectionState = "SIDEWAYS"
	DirBearish       DirectionState = "BEARISH"
	DirStrongBearish DirectionState = "STRONG_BEARISH"
)

type TickEnrichment struct {
	Timestamp   time.Time      `json:"timestamp"`
	MinuteIndex int            `json:"minute_index"`
	VolumeRank  int            `json:"volume_rank"`
	TickRank    int            `json:"tick_rank"`
	RangeRank   int            `json:"range_rank"`
	PriceRank   int            `json:"price_rank"`
	Direction   DirectionState `json:"direction"`
}

type EnrichedTick struct {
	Raw         TickData           `json:"raw"`
	TickVolume  int64              `json:"tick_volume"`
	MinuteIndex int                `json:"minute_index"`
	Enrichment  TickEnrichment     `json:"enrichment"`
	VolProfile  *VolumeProfileInfo `json:"vol_profile,omitempty"`
}
