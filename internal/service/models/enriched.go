package models

import "time"

type SimplifiedEnrichment struct {
	Timestamp   time.Time `json:"timestamp"`
	MinuteIndex int       `json:"minute_index"`
	VolumeRank  int       `json:"volume_rank"`
	TickRank    int       `json:"tick_rank"`
	PriceRank   int       `json:"price_rank"`
}

type EnrichedTick struct {
	Raw         TickData             `json:"raw"`
	TickVolume  int64                `json:"tick_volume"`
	MinuteIndex int                  `json:"minute_index"`
	Enrichment  SimplifiedEnrichment `json:"enrichment"`
	VolProfile  *VolumeProfileInfo   `json:"vol_profile,omitempty"`
}
