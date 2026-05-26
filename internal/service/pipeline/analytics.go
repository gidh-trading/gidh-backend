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
	dbWriter *writer.DBWriter // Injected for asynchronous hypertable archiving
	wsHub    *ws.Hub          // Injected for asset-specific stream broadcasting
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

// Analyze processes incoming raw ticks to manage the lifecycle of institutional volume regimes.
// It handles real-time data streaming over WebSockets and handles background data persistence.
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	volRank := tick.Enrichment.VolumeRank
	currentPrice := tick.Raw.LastPrice
	minuteIndex := tick.MinuteIndex
	currentTimestamp := tick.Enrichment.Timestamp

	// Fetch or allocate the memory tracking window for this asset safely
	session, exists := ae.sessions[token]
	if !exists {
		session = &models.VolumeRegimeSession{
			Token:     token,
			StockName: tick.Raw.StockName,
		}
		ae.sessions[token] = session
	}

	// 1. PHASE 2: BURST EVOLUTION (Volume Burst Threshold >= Rank 6 / P90 benchmark)
	if volRank >= 6 {
		if !session.Active {
			// Birth: Initialize fresh continuous participation window
			session.Active = true
			session.StartPrice = currentPrice
			session.StartTime = currentTimestamp
			session.StartMinuteIndex = minuteIndex
			session.PeakVolumeRank = volRank
		}

		if volRank > session.PeakVolumeRank {
			session.PeakVolumeRank = volRank
		}

		session.CurrentPrice = currentPrice

		// Extract directional delta and absolute displacement magnitude
		signedMove := session.CurrentPrice - session.StartPrice
		absMove := math.Abs(signedMove)

		// Compute elapsed session duration in minutes (fallback to 1 sec minimum to eliminate divide-by-zero)
		durationMinutes := currentTimestamp.Sub(session.StartTime).Minutes()
		if durationMinutes < 0.0166 {
			durationMinutes = 1.0 / 60.0
		}

		// Compute velocity per minute and look up multi-minute time-weighted DNA ranks
		displacementVelocity := absMove / durationMinutes
		currentPriceRank := ae.calculateBlendedPriceRank(token, session.StartMinuteIndex, minuteIndex, durationMinutes, displacementVelocity)

		// Pack dynamic live updates into a snapshot view layer struct
		snapshot := models.VolumeRegimeSnapshot{
			Timestamp:        currentTimestamp,
			InstrumentToken:  token,
			StockName:        session.StockName,
			MinuteIndex:      minuteIndex,
			Active:           true,
			AnomalyType:      models.AnomalyVolumeBurst,
			Direction:        ae.deduceDirection(signedMove),
			StartTime:        session.StartTime,
			EndTime:          currentTimestamp,
			StartPrice:       session.StartPrice,
			CurrentPrice:     session.CurrentPrice,
			SignedMove:       signedMove,
			AbsMove:          absMove,
			PeakVolumeRank:   session.PeakVolumeRank,
			CurrentPriceRank: currentPriceRank,
		}

		// STREAM ASSET-SPECIFIC REALTIME EVENT TO THE UI: Route strictly to this asset's regimes channel
		if ae.wsHub != nil {
			wsRoomKey := snapshot.StockName + ":regimes"
			ae.wsHub.BroadcastJSON(wsRoomKey, map[string]any{
				"type": "volume_regime_update",
				"data": snapshot,
			})
		}

		return
	}

	// 2. PHASE 3: DEATH & LIQUIDITY ABSORPTION CLASSIFICATION
	// Triggered on the exact tick participation subsides below benchmark parameters.
	if session.Active && volRank < 6 {
		signedMove := session.CurrentPrice - session.StartPrice
		absMove := math.Abs(signedMove)

		durationMinutes := currentTimestamp.Sub(session.StartTime).Minutes()
		if durationMinutes < 0.0166 {
			durationMinutes = 1.0 / 60.0
		}

		// Compute total move velocity across the entire spanned lifecycle
		displacementVelocity := absMove / durationMinutes
		finalPriceRank := ae.calculateBlendedPriceRank(token, session.StartMinuteIndex, minuteIndex, durationMinutes, displacementVelocity)

		snapshot := models.VolumeRegimeSnapshot{
			Timestamp:        currentTimestamp,
			InstrumentToken:  token,
			StockName:        session.StockName,
			MinuteIndex:      minuteIndex,
			Active:           false, // Signals concluded frame milestone
			Direction:        ae.deduceDirection(signedMove),
			StartTime:        session.StartTime,
			EndTime:          currentTimestamp,
			StartPrice:       session.StartPrice,
			CurrentPrice:     session.CurrentPrice,
			SignedMove:       signedMove,
			AbsMove:          absMove,
			PeakVolumeRank:   session.PeakVolumeRank,
			CurrentPriceRank: finalPriceRank,
		}

		// Core Microstructure Logic Matrix: High volume paired with weak velocity flags passive absorption
		if finalPriceRank <= 3 {
			snapshot.AnomalyType = models.AnomalyAbsorption
		} else {
			snapshot.AnomalyType = models.AnomalyVolumeBurst
		}

		// BROADCAST TERMINATION STATE: Notify chart layers one final time to paint permanent anchors
		if ae.wsHub != nil {
			wsRoomKey := snapshot.StockName + ":regimes"
			ae.wsHub.BroadcastJSON(wsRoomKey, map[string]any{
				"type": "volume_regime_update",
				"data": snapshot,
			})
		}

		// ASYNCHRONOUS DATABASE STORAGE WRITER ROUTING: Fire-and-forget to background TimescaleDB thread
		if ae.dbWriter != nil {
			go func(snap models.VolumeRegimeSnapshot) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = ae.dbWriter.SaveVolumeRegimeSession(ctx, &snap)
			}(snapshot)
		}

		// Cleanly wipe memory structures to reset allocation pointers for the next burst sequence
		session.Active = false
		session.StartPrice = 0.0
		session.CurrentPrice = 0.0
		session.PeakVolumeRank = 0
		session.StartMinuteIndex = 0

		return
	}
}

// calculateBlendedPriceRank averages DNA thresholds across a multi-minute index span to accurately benchmark cumulative velocity.
func (ae *AnalyticsEngine) calculateBlendedPriceRank(token uint32, startMin, endMin int, durationMinutes, velocityValue float64) int {
	dna, found := ae.dnaMap[token]
	if !found || dna == nil {
		return 4 // Balanced baseline coordinate fallback if asset mapping profile is missing
	}

	// Route intra-minute parameters straight to the localized tracker to prevent cross-boundary dilution
	if durationMinutes <= 1.0 || startMin == endMin {
		return ae.calculateSinglePriceRank(dna, endMin, velocityValue)
	}

	if startMin > endMin {
		startMin, endMin = endMin, startMin
	}

	// Construct rapid O(1) loop-up mapping index to protect against non-continuous array row records gaps
	bucketMap := make(map[int]*models.TimeBucketDNA)
	for i := range dna.TimeBuckets {
		bucketMap[dna.TimeBuckets[i].MinuteIndex] = &dna.TimeBuckets[i]
	}

	var sumP05, sumP10, sumP25, sumP50, sumP75, sumP90, sumP97 float64
	var count float64

	// Accumulate thresholds across the exact chronological sequence traveled by the anomaly
	for m := startMin; m <= endMin; m++ {
		bucket, match := bucketMap[m]
		if match && bucket != nil {
			sumP05 += bucket.PriceP05
			sumP10 += bucket.PriceP10
			sumP25 += bucket.PriceP25
			sumP50 += bucket.PriceP50
			sumP75 += bucket.PriceP75
			sumP90 += bucket.PriceP90
			sumP97 += bucket.PriceP97
			count++
		}
	}

	// Fallback cleanly to local evaluation if index matrix bounds don't match data rows
	if count == 0 {
		return ae.calculateSinglePriceRank(dna, endMin, velocityValue)
	}

	// Extract balanced duration-weighted baseline vectors
	avgP97 := sumP97 / count
	avgP90 := sumP90 / count
	avgP75 := sumP75 / count
	avgP50 := sumP50 / count
	avgP25 := sumP25 / count
	avgP10 := sumP10 / count

	return ae.evalThresholds(velocityValue, avgP97, avgP90, avgP75, avgP50, avgP25, avgP10)
}

// calculateSinglePriceRank targets a single specific minute bucket to rank instantaneous velocity.
func (ae *AnalyticsEngine) calculateSinglePriceRank(dna *models.MarketDNA, targetMin int, velocityValue float64) int {
	var bucket *models.TimeBucketDNA
	for i := range dna.TimeBuckets {
		if dna.TimeBuckets[i].MinuteIndex == targetMin {
			bucket = &dna.TimeBuckets[i]
			break
		}
	}

	if bucket == nil {
		return 4
	}

	return ae.evalThresholds(
		velocityValue,
		bucket.PriceP97,
		bucket.PriceP90,
		bucket.PriceP75,
		bucket.PriceP50,
		bucket.PriceP25,
		bucket.PriceP10,
	)
}

// evalThresholds maps execution velocity directly onto linear ranks 1-7.
func (ae *AnalyticsEngine) evalThresholds(velocityValue, p97, p90, p75, p50, p25, p10 float64) int {
	switch {
	case velocityValue >= p97:
		return 7 // Extreme Extension Velocity
	case velocityValue >= p90:
		return 6 // Significant Velocity
	case velocityValue >= p75:
		return 5 // Active Velocity
	case velocityValue >= p50:
		return 4 // Normal structural velocity
	case velocityValue >= p25:
		return 3 // Retained Absorption Velocity Threshold
	case velocityValue >= p10:
		return 2 // Compressed response velocity
	default:
		return 1 // Absolute structural deadlock velocity
	}
}

// deduceDirection maps raw signed deltas into type-safe structural directional enums.
func (ae *AnalyticsEngine) deduceDirection(signedMove float64) models.RegimeDirection {
	if signedMove > 0 {
		return models.DirectionBullish // Buying Initiative / Passive Sell Absorption
	}
	if signedMove < 0 {
		return models.DirectionBearish // Selling Initiative / Passive Buy Absorption
	}
	return models.DirectionFlat // Absolute structural boundary consolidation or lock
}
