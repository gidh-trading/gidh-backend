package pipeline

import (
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type AnalyticsEngine struct {
	mu       sync.RWMutex
	dnaMap   map[uint32]*models.MarketDNA
	profiles map[uint32]*models.InstrumentProfile
	dbWriter *writer.DBWriter
	wsHub    *ws.Hub
}

func NewAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA, profiles map[uint32]*models.InstrumentProfile, db *writer.DBWriter, hub *ws.Hub) *AnalyticsEngine {
	if dnaMap == nil {
		dnaMap = make(map[uint32]*models.MarketDNA)
	}
	return &AnalyticsEngine{
		dnaMap:   dnaMap,
		profiles: profiles,
		dbWriter: db,
		wsHub:    hub,
	}
}

func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	volRank := tick.Enrichment.VolumeRank
	priceRank := tick.Enrichment.PriceRank

	// 1. 🧠 INSTANTANEOUS PLUG-AND-PLAY ANOMALY CHECK
	// If volume rank indicates institutional activity (>= 6)
	var currentAnomaly models.AnomalyType = models.AnomalyNone
	if volRank >= 6 {
		// If price range expansion is stuck/suppressed, it's absorption
		if priceRank <= 3 {
			currentAnomaly = models.AnomalyAbsorption
		} else {
			currentAnomaly = models.AnomalyVolumeBurst
		}
	}

	// 2. Attach anomaly classification straight to the tick for downstream stages (like BarManager)
	//tick.Anomaly = currentAnomaly

	// 3. Optional: Emit an instantaneous real-time engine signal straight to UI via WebSockets
	if ae.wsHub != nil && currentAnomaly != models.AnomalyNone {
		ae.wsHub.BroadcastJSON(tick.Raw.StockName+":engine_anomaly", map[string]any{
			"type":         "realtime_anomaly",
			"token":        tick.Raw.InstrumentToken,
			"ticker":       tick.Raw.StockName,
			"time":         tick.Raw.Timestamp,
			"price":        tick.Raw.LastPrice,
			"volume_rank":  volRank,
			"price_rank":   priceRank,
			"anomaly_type": currentAnomaly.String(),
		})
	}
}

func (ae *AnalyticsEngine) evalThresholds(velocityValue, p97, p90, p75, p50, p25, p10 float64) int {
	switch {
	case velocityValue >= p97:
		return 7
	case velocityValue >= p90:
		return 6
	case velocityValue >= p75:
		return 5
	case velocityValue >= p50:
		return 4
	case velocityValue >= p25:
		return 3
	case velocityValue >= p10:
		return 2
	default:
		return 1
	}
}
