// internal/service/pipeline/bar_analytics.go
package pipeline

import (
	"gidh-backend/internal/service/models"
)

type BarAnalyticsEngine struct{}

func NewBarAnalyticsEngine() *BarAnalyticsEngine {
	return &BarAnalyticsEngine{}
}

// AnalyzeTick evaluates market microstructure on every single tick update
func (bae *BarAnalyticsEngine) AnalyzeTick(bar *models.Bar, tick *models.EnrichedTick) {

}

// AnalyzeClose applies macro filters right before archiving the finalized segment
func (bae *BarAnalyticsEngine) AnalyzeClose(bar *models.Bar) {

}
