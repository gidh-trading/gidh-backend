// internal/service/pipeline/analytics.go

package pipeline

import (
	"math"

	"gidh-backend/internal/service/models"
)

type AnalyticsStage struct {
	profiles map[uint32]*models.InstrumentProfile
}

func NewAnalyticsStage(profiles map[uint32]*models.InstrumentProfile) *AnalyticsStage {
	return &AnalyticsStage{
		profiles: profiles,
	}
}

func (s *AnalyticsStage) Process(tick *models.EnrichedTick) error {
	// Evaluate institutional volume burst rules using simultaneous metrics
	if tick.VolumeZ > 2.0 && tick.RelativeVolume > 2.5 {
		token := tick.Raw.InstrumentToken
		price := tick.Raw.LastPrice
		bucketSize := 1.0

		// Extract configuration bucket matrix step sizes
		if prof, ok := s.profiles[token]; ok && prof.BucketSize > 0 {
			bucketSize = prof.BucketSize
		}

		// Snap price level to horizontal coordinates
		tick.HasAnomaly = true
		tick.AnomalyBin = math.Floor(price/bucketSize) * bucketSize
	} else {
		tick.HasAnomaly = false
	}

	return nil
}
