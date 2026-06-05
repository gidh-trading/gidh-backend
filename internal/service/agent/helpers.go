package agent

import "gidh-backend/internal/service/models"

func (rm *RiskManager) IngestClosedBar(bar *models.Bar) {
	rm.scalper.IngestClosedBar(bar)
}
