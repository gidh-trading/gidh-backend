package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

// HistoricalPricePoint tracks an isolated price observation in time for rolling trend metrics
type HistoricalPricePoint struct {
	Timestamp time.Time
	Price     float64
}

// TokenTrendContext preserves the continuous, bar-independent price history per instrument
type TokenTrendContext struct {
	HistoricalPrices []HistoricalPricePoint
}

type AnalyticsEngine struct {
	tokenTrends map[uint32]*TokenTrendContext
	mu          sync.Mutex
}

func NewAnalyticsEngine() *AnalyticsEngine {
	return &AnalyticsEngine{
		tokenTrends: make(map[uint32]*TokenTrendContext),
	}
}

// Analyze evaluates live telemetry microstructural properties independent of fixed candle timelines.
// It returns a type-safe AnomalySnapshot for event logging and a TrendMetrics struct for tracking velocity.
func (ae *AnalyticsEngine) Analyze(tick *models.EnrichedTick) (models.AnomalySnapshot, models.TrendMetrics) {
	ae.mu.Lock()
	defer ae.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	now := tick.Enrichment.Timestamp

	// ----------------------------------------------------------------
	// 1. STATEFUL CONTINUOUS TREND CALCULATION (Rolling Window of Rolling Metrics)
	// ----------------------------------------------------------------
	ctx, exists := ae.tokenTrends[token]
	if !exists {
		// Pre-allocate capacity for frequent updates across a 10-minute trading horizon
		ctx = &TokenTrendContext{HistoricalPrices: make([]HistoricalPricePoint, 0, 600)}
		ae.tokenTrends[token] = ctx
	}

	// Append current observation to the trend queue
	ctx.HistoricalPrices = append(ctx.HistoricalPrices, HistoricalPricePoint{Timestamp: now, Price: price})

	// Evict historical points older than 10 minutes to maintain our sliding trend window
	cutoff := now.Add(-10 * time.Minute)
	evictIdx := 0
	for evictIdx < len(ctx.HistoricalPrices) && ctx.HistoricalPrices[evictIdx].Timestamp.Before(cutoff) {
		evictIdx++
	}
	if evictIdx > 0 {
		ctx.HistoricalPrices = ctx.HistoricalPrices[evictIdx:]
	}

	// Compute underlying trend velocity
	trend := models.TrendMetrics{
		PriceTrendDirection: 0,
		TenMinuteNetReturn:  0.0,
		VelocityPerMinute:   0.0,
	}

	if len(ctx.HistoricalPrices) > 1 {
		tenMinsAgoPrice := ctx.HistoricalPrices[0].Price
		netChange := price - tenMinsAgoPrice

		trend.TenMinuteNetReturn = netChange
		trend.VelocityPerMinute = netChange / 10.0

		// Assign directional flags based on organic point movement thresholds
		if netChange > 0.05 {
			trend.PriceTrendDirection = 1 // Up-Trend
		} else if netChange < -0.05 {
			trend.PriceTrendDirection = -1 // Down-Trend
		}
	}

	// ----------------------------------------------------------------
	// 2. TYPE-SAFE INSTITUTIONAL ANOMALY DETECTION (Enum Classification)
	// ----------------------------------------------------------------
	volRank := getPercentileRank(tick.Enrichment.VolumePercentile)
	priceRank := getPercentileRank(tick.Enrichment.PricePercentile)
	displacement := tick.Telemetry.LiveDisplacement

	// Initialize a lean, default snapshot footprint targeting our type-safe enums
	snapshot := models.AnomalySnapshot{
		Timestamp:  now,
		Type:       models.AnomalyNone, // Default to safe fallback state
		Direction:  0,
		VolumeRank: volRank,
		PriceRank:  priceRank,
	}

	// Gatekeeper check: Identify severe institutional activity (P90 = Rank 6, P97 = Rank 7)
	if volRank >= 6 {
		// Assign basic directional breakout attributes to the volume anomaly
		snapshot.Type = models.AnomalyVolumeBurst
		if displacement > 0 {
			snapshot.Direction = 1
		} else if displacement < 0 {
			snapshot.Direction = -1
		}

		// Microstructural Absorption Check: Elevated volume matched with capped/stalled price metrics
		if priceRank <= 3 { // P25 or weaker localized price impact
			snapshot.Type = models.AnomalyAbsorption // Upgrade category classification to compile-safe enum

			// Determine context bias: if price is flat/down on heavy buying pressure vs flat/up on selling
			if displacement >= 0 {
				snapshot.Direction = 1 // Passive supply accumulation (Buy Absorption)
			} else {
				snapshot.Direction = -1 // Passive demand distribution (Sell Absorption)
			}
		}
	}

	return snapshot, trend
}
