package pipeline

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
)

type InstrumentContext struct {
	LastVolume    int64
	LastPrice     float64
	CurrentBarMin int     // Tracks the active clock minute locally for real-time tracking
	BarOpenPrice  float64 // Captures the discrete open price for exact DNA range matching
	Buffer        *TokenRollingBuffer
	DNA           map[int]models.TimeBucketDNA
}

type EnrichmentStage struct {
	instruments     map[uint32]*InstrumentContext
	positionManager order.PositionManager
	loc             *time.Location
	mu              sync.Mutex
}

// NewEnrichmentStage maps baseline definitions and injects the active position manager
func NewEnrichmentStage(pm order.PositionManager, rawDnaMap map[uint32]*models.MarketDNA) *EnrichmentStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	instruments := make(map[uint32]*InstrumentContext)

	for token, dna := range rawDnaMap {
		fastDnaMap := make(map[int]models.TimeBucketDNA)
		for _, bucket := range dna.TimeBuckets {
			fastDnaMap[bucket.MinuteIndex] = bucket
		}

		instruments[token] = &InstrumentContext{
			Buffer:        NewTokenRollingBuffer(),
			DNA:           fastDnaMap,
			LastVolume:    0,
			LastPrice:     0.0,
			CurrentBarMin: -1,
			BarOpenPrice:  0.0,
		}
	}

	return &EnrichmentStage{
		instruments:     instruments,
		positionManager: pm,
		loc:             loc,
	}
}

func (s *EnrichmentStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	ts := tick.Raw.Timestamp.In(s.loc)

	ctx, exists := s.instruments[token]
	if !exists {
		ctx = &InstrumentContext{
			Buffer:        NewTokenRollingBuffer(),
			DNA:           make(map[int]models.TimeBucketDNA),
			LastVolume:    0,
			LastPrice:     price,
			CurrentBarMin: ts.Minute(),
			BarOpenPrice:  price,
		}
		s.instruments[token] = ctx
	}

	// 1. Compute standalone cumulative tick volume adjustments
	curr := tick.Raw.CumulativeVolume
	prev := ctx.LastVolume
	var delta int64
	if prev == 0 || curr < prev {
		delta = tick.Raw.LastTradedQuantity
	} else {
		delta = curr - prev
	}
	ctx.LastVolume = curr
	tick.TickVolume = delta

	// Drop dead/idle ticks
	if tick.TickVolume == 0 && price == ctx.LastPrice {
		return nil
	}

	// 2. Real-time strategy signal execution trigger routing
	if price != ctx.LastPrice && s.positionManager != nil {
		s.positionManager.OnPriceUpdate(tick.Raw.StockName, tick.Raw.LastPrice, tick.Raw.Timestamp)
	}
	ctx.LastPrice = price

	// 3. Update the sliding 60-second real-time participation metrics (Volume & Ticks remain rolling)
	ctx.Buffer.Push(ts, price, float64(delta))
	liveVolume, liveTickCount, _ := ctx.Buffer.GetProductionMetrics()

	// 4. Manage discrete candle opening references locally in memory for real-time tracking
	currentClockMin := ts.Minute()
	if ctx.CurrentBarMin == -1 || currentClockMin != ctx.CurrentBarMin {
		ctx.CurrentBarMin = currentClockMin
		ctx.BarOpenPrice = price // Lock in the opening price for this specific minute index
	}

	minOfDay := (ts.Hour() * 60) + ts.Minute()
	minuteIndex := minOfDay - 555
	tick.MinuteIndex = minuteIndex

	// 5. Run non-Gaussian historical normalization mapping
	volRank := 4
	tickRank := 4
	priceRank := 4 // Default baseline structural velocity floor

	if baseline, ok := ctx.DNA[minuteIndex]; ok {
		// Calculate Volume Rank
		switch {
		case liveVolume >= baseline.VolumeP97:
			volRank = 7
		case liveVolume >= baseline.VolumeP90:
			volRank = 6
		case liveVolume >= baseline.VolumeP75:
			volRank = 5
		case liveVolume >= baseline.VolumeP50:
			volRank = 4
		case liveVolume >= baseline.VolumeP25:
			volRank = 3
		case liveVolume >= baseline.VolumeP10:
			volRank = 2
		default:
			volRank = 1
		}

		// Calculate Tick Churn Intensity Rank
		floatTickCount := float64(liveTickCount)
		switch {
		case floatTickCount >= baseline.TickCountP97:
			tickRank = 7
		case floatTickCount >= baseline.TickCountP90:
			tickRank = 6
		case floatTickCount >= baseline.TickCountP75:
			tickRank = 5
		case floatTickCount >= baseline.TickCountP50:
			tickRank = 4
		case floatTickCount >= baseline.TickCountP25:
			tickRank = 3
		case floatTickCount >= baseline.TickCountP10:
			tickRank = 2
		default:
			tickRank = 1
		}

		// 🔥 APPLES-TO-APPLES FIX: Calculate absolute discrete distance from the local minute open price
		absCandleRange := math.Abs(price - ctx.BarOpenPrice)

		if baseline.PriceP50 > 0 && absCandleRange < baseline.PriceP50 {
			// If the price change is under the historical median, map it into compression ranks
			switch {
			case absCandleRange >= baseline.PriceP25:
				priceRank = 3 // Suppressed Expansion / Anomaly Absorption Zone
			case absCandleRange >= baseline.PriceP10:
				priceRank = 2 // Low Volatility Churn Space
			default:
				priceRank = 1 // Absolute Deadlock State
			}
		} else {
			// Evaluate active breakout expansions cleanly against upper boundaries
			switch {
			case absCandleRange >= baseline.PriceP97:
				priceRank = 7 // True Extreme Breakout Velocity (Saturated Magenta)
			case absCandleRange >= baseline.PriceP90:
				priceRank = 6 // Significant Velocity
			case absCandleRange >= baseline.PriceP75:
				priceRank = 5 // Active Expansion
			default:
				priceRank = 4 // Normal Structural Mean (Yellow)
			}
		}
	}

	// Pack final metrics safely for downstream consumer stages (Analytics & Bar Manager)
	tick.Enrichment = models.SimplifiedEnrichment{
		Timestamp:   ts,
		MinuteIndex: minuteIndex,
		VolumeRank:  volRank,
		TickRank:    tickRank,
		PriceRank:   priceRank,
	}

	return nil
}
