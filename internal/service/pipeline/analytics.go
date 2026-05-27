package pipeline

import (
	"context"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type AnalyticsEngine struct {
	mu       sync.RWMutex
	sessions map[uint32]*models.VolumeRegimeSession
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
		sessions: make(map[uint32]*models.VolumeRegimeSession),
		dnaMap:   dnaMap,
		profiles: profiles,
		dbWriter: db,
		wsHub:    hub,
	}
}

func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	currentPrice := tick.Raw.LastPrice
	volRank := tick.Enrichment.VolumeRank
	priceRank := tick.Enrichment.PriceRank
	tickRank := tick.Enrichment.TickRank

	session, active := ae.sessions[token]

	// 1. REGIME BIRTH
	if !active {
		if volRank >= 6 {
			direction := models.DirectionFlat

			newSession := &models.VolumeRegimeSession{
				Active:           true,
				Token:            token,
				StockName:        tick.Raw.StockName,
				StartPrice:       currentPrice,
				CurrentPrice:     currentPrice,
				LastTickPrice:    currentPrice,
				SessionHigh:      currentPrice,
				SessionLow:       currentPrice,
				StartTime:        tick.Raw.Timestamp,
				StartMinuteIndex: tick.MinuteIndex,
				PeakVolumeRank:   volRank,
				PeakTickRank:     tickRank,
				PeakPriceRank:    priceRank,
				Direction:        direction,
			}
			ae.sessions[token] = newSession

			if ae.wsHub != nil {
				ae.wsHub.BroadcastJSON(tick.Raw.StockName+":regime", map[string]any{
					"type":   "regime_start",
					"token":  token,
					"ticker": tick.Raw.StockName,
					"time":   tick.Raw.Timestamp,
					"price":  currentPrice,
				})
			}
		}
		return
	}

	// 2. REGIME CONTINUUM
	if session.Direction == models.DirectionFlat && currentPrice != session.LastTickPrice {
		session.Direction = ae.deduceDirection(currentPrice - session.LastTickPrice)
	}

	session.CurrentPrice = currentPrice

	if currentPrice > session.SessionHigh {
		session.SessionHigh = currentPrice
	}
	if currentPrice < session.SessionLow {
		session.SessionLow = currentPrice
	}

	if volRank > session.PeakVolumeRank {
		session.PeakVolumeRank = volRank
	}
	if tickRank > session.PeakTickRank {
		session.PeakTickRank = tickRank
	}
	if priceRank > session.PeakPriceRank {
		session.PeakPriceRank = priceRank
	}

	session.LastTickPrice = currentPrice

	// 3. REGIME DEATH STATE EVALUATION WITH TIME HYSTERESIS
	// If participation falls below threshold, wait to confirm it isn't a temporary pause
	if volRank <= 3 {
		// 🟢 FIX 1: If this is the first tick below threshold, tag it but don't close yet
		if session.LastUnderThreshold.IsZero() {
			session.LastUnderThreshold = tick.Raw.Timestamp
			return
		}

		// 🟢 FIX 2: Check if the cool-off window has fully elapsed (e.g., 10 seconds grace period)
		if tick.Raw.Timestamp.Sub(session.LastUnderThreshold) < 10*time.Second {
			return
		}
	} else {
		// Reset cool-off timestamp if volume picks back up
		session.LastUnderThreshold = time.Time{}
	}

	// Terminate Session
	sessionDuration := tick.Raw.Timestamp.Sub(session.StartTime)

	// 🟢 FIX 3: FILTER INFANT NOISE (Ignore sessions shorter than 15 seconds)
	if sessionDuration < 15*time.Second {
		delete(ae.sessions, token)
		return
	}

	signedMove := session.CurrentPrice - session.StartPrice
	absMove := math.Abs(signedMove)

	var finalizedAnomaly models.AnomalyType = models.AnomalyVolumeBurst

	if prof, ok := ae.profiles[token]; ok && prof != nil && prof.ATR14 > 0 {
		macroSessionRange := session.SessionHigh - session.SessionLow
		sessionVolatilityFactor := macroSessionRange / prof.ATR14

		// 🟢 FIX 4: Use macro tracking metrics contextually scaled for duration
		// For longer block executions, absorption typically holds range to <= 4% of ATR
		if sessionVolatilityFactor <= 0.04 {
			finalizedAnomaly = models.AnomalyAbsorption
		}
	} else {
		if session.PeakPriceRank <= 3 {
			finalizedAnomaly = models.AnomalyAbsorption
		}
	}

	snapshot := &models.VolumeRegimeSnapshot{
		Timestamp:        tick.Raw.Timestamp,
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

	if ae.dbWriter != nil {
		go func(snap *models.VolumeRegimeSnapshot) {
			_ = ae.dbWriter.SaveVolumeRegimeSnapshot(context.Background(), snap)
		}(snapshot)
	}

	if ae.wsHub != nil {
		ae.wsHub.BroadcastJSON(session.StockName+":regime", map[string]any{
			"type":    "regime_end",
			"anomaly": finalizedAnomaly.String(),
			"token":   session.Token,
			"ticker":  session.StockName,
			"data":    snapshot,
		})
	}

	delete(ae.sessions, token)
}

func (ae *AnalyticsEngine) deduceDirection(signedMove float64) models.RegimeDirection {
	if signedMove > 0 {
		return models.DirectionBullish
	}
	if signedMove < 0 {
		return models.DirectionBearish
	}
	return models.DirectionFlat
}

// Deprecated lookup functions preserved for compatibility
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
