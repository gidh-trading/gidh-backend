package pipeline

import (
	"context"
	"math"
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type AnalyticsEngine struct {
	mu       sync.RWMutex
	sessions map[uint32]*models.VolumeRegimeSession
	dnaMap   map[uint32]*models.MarketDNA
	dbWriter *writer.DBWriter
	wsHub    *ws.Hub
}

// NewAnalyticsEngine initializes the stateful tracking engine with historical DNA and runtime dependency injections.
func NewAnalyticsEngine(dnaMap map[uint32]*models.MarketDNA, db *writer.DBWriter, hub *ws.Hub) *AnalyticsEngine {
	if dnaMap == nil {
		dnaMap = make(map[uint32]*models.MarketDNA)
	}
	return &AnalyticsEngine{
		sessions: make(map[uint32]*models.VolumeRegimeSession),
		dnaMap:   dnaMap,
		dbWriter: db,
		wsHub:    hub,
	}
}

// Analyze processes incoming ticks to govern the stateful lifecycle of institutional volume regimes.
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	volRank := tick.Enrichment.VolumeRank
	priceRank := tick.Enrichment.PriceRank
	tickRank := tick.Enrichment.TickRank

	session, active := ae.sessions[token]

	// 1. REGIME BIRTH: Trigger active state tracking when Volume hits Rank 6 or 7
	if !active {
		if volRank >= 6 {
			direction := models.DirectionFlat
			prevClose := tick.Raw.LastPrice - tick.Raw.Change
			if prevClose > 0 {
				direction = ae.deduceDirection(tick.Raw.Change)
			}

			newSession := &models.VolumeRegimeSession{
				Active:           true,
				Token:            token,
				StockName:        tick.Raw.StockName,
				StartPrice:       tick.Raw.LastPrice,
				CurrentPrice:     tick.Raw.LastPrice,
				StartTime:        tick.Raw.Timestamp,
				StartMinuteIndex: tick.MinuteIndex,
				PeakVolumeRank:   volRank,
				PeakTickRank:     tickRank,
				PeakPriceRank:    priceRank,
				Direction:        direction,
			}
			ae.sessions[token] = newSession

			// Real-time notification layer push
			if ae.wsHub != nil {
				ae.wsHub.BroadcastJSON(tick.Raw.StockName+":regime", map[string]any{
					"type":   "regime_start",
					"token":  token,
					"ticker": tick.Raw.StockName,
					"time":   tick.Raw.Timestamp,
					"price":  tick.Raw.LastPrice,
				})
			}
		}
		return
	}

	// 2. REGIME CONTINUUM: Update step-by-step peaks to lock in maximum structural intensity metrics
	session.CurrentPrice = tick.Raw.LastPrice

	if volRank > session.PeakVolumeRank {
		session.PeakVolumeRank = volRank
	}
	if tickRank > session.PeakTickRank {
		session.PeakTickRank = tickRank
	}
	if priceRank > session.PeakPriceRank {
		session.PeakPriceRank = priceRank
	}

	// 3. REGIME DEATH: Conclude anomaly session when participation scores drop back to normal levels
	if volRank <= 3 {
		// Compile mathematical variables for the completed window
		signedMove := session.CurrentPrice - session.StartPrice
		absMove := math.Abs(signedMove)

		// 🔥 OBJECTIVE ABSORPTION ANOMALY CLASSIFICATION RULE
		// Institutional volume was immense, but the price vector failed to escape normal historical constraints (Rank <= 3)
		var finalizedAnomaly models.AnomalyType
		if session.PeakPriceRank <= 3 {
			finalizedAnomaly = models.AnomalyAbsorption
		} else {
			finalizedAnomaly = models.AnomalyVolumeBurst
		}

		// Map runtime memory variables onto the immutable snapshot model for database persistence
		snapshot := &models.VolumeRegimeSnapshot{
			Timestamp:        tick.Raw.Timestamp, // Hypertable partitioning criteria
			InstrumentToken:  int32(session.Token),
			StockName:        session.StockName,
			MinuteIndex:      tick.MinuteIndex,
			Active:           false,
			AnomalyType:      finalizedAnomaly,
			Direction:        session.Direction,
			StartTime:        session.StartTime,
			EndTime:          tick.Raw.Timestamp,
			StartPrice:       session.StartPrice,
			CurrentPrice:     session.CurrentPrice,
			SignedMove:       signedMove,
			AbsMove:          absMove,
			PeakVolumeRank:   session.PeakVolumeRank,
			CurrentPriceRank: session.PeakPriceRank,
		}

		// Asynchronously dispatch the dataset to the persistence layer
		if ae.dbWriter != nil {
			go func(snap *models.VolumeRegimeSnapshot) {
				_ = ae.dbWriter.SaveVolumeRegimeSnapshot(context.Background(), snap)
			}(snapshot)
		}

		// Emit structural event payloads to your UI heatmaps
		if ae.wsHub != nil {
			ae.wsHub.BroadcastJSON(session.StockName+":regime", map[string]any{
				"type":    "regime_end",
				"anomaly": finalizedAnomaly.String(),
				"token":   session.Token,
				"ticker":  session.StockName,
				"data":    snapshot,
			})
		}

		// Flush active memory token slot
		delete(ae.sessions, token)
	}
}

// deduceDirection evaluates price variation vectors to assign type-safe integer direction states
func (ae *AnalyticsEngine) deduceDirection(signedMove float64) models.RegimeDirection {
	if signedMove > 0 {
		return models.DirectionBullish
	}
	if signedMove < 0 {
		return models.DirectionBearish
	}
	return models.DirectionFlat
}

// Deprecated lookup functions preserved to ensure backwards compatibility with legacy interfaces
func (ae *AnalyticsEngine) evaluatePriceRank(token uint32, targetMin int, velocityValue float64) int {
	var bucket *models.TimeBucketDNA
	if dna, ok := ae.dnaMap[token]; ok {
		for i := range dna.TimeBuckets {
			if dna.TimeBuckets[i].MinuteIndex == targetMin {
				bucket = &dna.TimeBuckets[i]
				break
			}
		}
	}
	if bucket == nil {
		return 4
	}
	return ae.evalThresholds(velocityValue, bucket.PriceP97, bucket.PriceP90, bucket.PriceP75, bucket.PriceP50, bucket.PriceP25, bucket.PriceP10)
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
