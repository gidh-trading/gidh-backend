// internal/service/pipeline/hq.go
package pipeline

import (
	"context"
	"sync"

	"gidh-backend/internal/service/analytics"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"

	"github.com/jackc/pgx/v5/pgxpool"
)

type StockMemory struct {
	TickBuffer []models.TickData
	State      models.HqIntelligencePayload

	// mu MUST be changed to RWMutex to unlock RLock() and RUnlock() capability
	mu sync.RWMutex

	// ---- Structural Canvas Trackers ----
	ActiveBarMinuteIndex int                     // Tracking interval boundary
	DiscoveredWalls      []models.AbsorptionWall // Remembers every anomaly hit inside the current bar
}

type Headquarters struct {
	pool          *pgxpool.Pool
	positionCtx   order.PositionManager
	stocks        map[uint32]*StockMemory
	stocksMu      sync.RWMutex
	dnaMap        map[uint32]*models.MarketDNA // Injected session context mapping
	lookbackTicks int
}

func NewHeadquarters(pool *pgxpool.Pool, pm order.PositionManager, dnaMap map[uint32]*models.MarketDNA, lookback int) *Headquarters {
	if lookback <= 0 {
		lookback = 300 // Tracks continuous tape dynamics across a baseline 300-tick window
	}
	return &Headquarters{
		pool:          pool,
		positionCtx:   pm,
		stocks:        make(map[uint32]*StockMemory),
		dnaMap:        dnaMap, // Cached natively on app startup sequences
		lookbackTicks: lookback,
	}
}

func (h *Headquarters) IngestPipelineTick(ctx context.Context, tick *models.EnrichedTick) {
	token := tick.Raw.InstrumentToken

	h.stocksMu.Lock()
	mem, exists := h.stocks[token]
	if !exists {
		mem = &StockMemory{
			TickBuffer: make([]models.TickData, 0, 100000),
			State: models.HqIntelligencePayload{
				Direction: "NONE",
				FlowMetrics: models.TapeTelemetryUnits{
					IsAbsorption: false,
					ActiveWalls:  []models.AbsorptionWall{},
				},
			},
			ActiveBarMinuteIndex: -1,
			DiscoveredWalls:      []models.AbsorptionWall{},
		}
		h.stocks[token] = mem
	}
	dnaProfile := h.dnaMap[token]
	h.stocksMu.Unlock()

	mem.mu.Lock()
	defer mem.mu.Unlock()

	// 1. Structural Boundary Check: Reset memory when a brand new bar interval forms
	if mem.ActiveBarMinuteIndex != tick.MinuteIndex {
		mem.ActiveBarMinuteIndex = tick.MinuteIndex
		mem.DiscoveredWalls = []models.AbsorptionWall{} // Wipe old walls for the new bar window
	}

	// Append raw data to sample window
	mem.TickBuffer = append(mem.TickBuffer, tick.Raw)
	bufferSize := len(mem.TickBuffer)

	var sample []models.TickData
	if bufferSize <= h.lookbackTicks {
		sample = mem.TickBuffer
	} else {
		sample = mem.TickBuffer[bufferSize-h.lookbackTicks : bufferSize]
	}

	telemetry := analytics.CalculateHybridTelemetry(sample, h.lookbackTicks, dnaProfile)

	// Extract sliding ranks from the 1m enrichment container
	volRank := tick.Enrichment.VolumeRank
	priceRank := tick.Enrichment.PriceRank

	const lowEfficiencyLimit = 0.05
	currentPrice := tick.Raw.LastPrice

	// 2. Real-Time Multi-Tier Matrix Evaluation
	if volRank >= 6 && priceRank <= 3 && telemetry.Efficiency <= lowEfficiencyLimit {

		if telemetry.BiasScore >= 0.50 {
			// Aggressive buying hit short wall
			h.registerUniqueWall(&mem.DiscoveredWalls, "ABSORPTION_SHORT", currentPrice)
			mem.State.Direction = "ABSORPTION_SHORT"

		} else if telemetry.BiasScore <= -0.50 {
			// Aggressive selling hit buy floor
			h.registerUniqueWall(&mem.DiscoveredWalls, "ABSORPTION_LONG", currentPrice)
			mem.State.Direction = "ABSORPTION_LONG"
		}
	} else {
		// Use standard trend tracking if no fresh anomaly is overwhelming the current tick
		if telemetry.BiasScore >= 0.25 {
			mem.State.Direction = "BULLISH"
		} else if telemetry.BiasScore <= -0.25 {
			mem.State.Direction = "BEARISH"
		} else {
			mem.State.Direction = "NONE"
		}
	}

	// 3. Keep the payload populated with ALL discovered walls for the current bar
	if len(mem.DiscoveredWalls) > 0 {
		mem.State.FlowMetrics.IsAbsorption = true
		mem.State.FlowMetrics.ActiveWalls = mem.DiscoveredWalls
	} else {
		mem.State.FlowMetrics.IsAbsorption = false
		mem.State.FlowMetrics.ActiveWalls = []models.AbsorptionWall{}
	}

	mem.State.FlowMetrics.BiasScore = telemetry.BiasScore
	mem.State.FlowMetrics.VwpDelta = telemetry.VwpDelta
	mem.State.FlowMetrics.Efficiency = telemetry.Efficiency
}

// Helper routine to append a wall coordinate only if it hasn't been cached already
func (h *Headquarters) registerUniqueWall(walls *[]models.AbsorptionWall, direction string, price float64) {
	for _, w := range *walls {
		// Avoid duplicating bubbles at the exact same coordinate layer
		if w.Direction == direction && w.AbsorptionPrice == price {
			return
		}
	}
	*walls = append(*walls, models.AbsorptionWall{
		Direction:       direction,
		AbsorptionPrice: price,
	})
}

// GetIntelligenceSnapshot performs a thread-safe, non-blocking read operation
// to serve live structural intelligence snapshots to the downstream BarManager.
func (h *Headquarters) GetIntelligenceSnapshot(token uint32) models.HqIntelligencePayload {
	h.stocksMu.RLock()
	mem, exists := h.stocks[token]
	h.stocksMu.RUnlock()

	if !exists || mem == nil {
		return models.HqIntelligencePayload{
			Direction: "NONE",
			FlowMetrics: models.TapeTelemetryUnits{
				IsAbsorption: false,
				ActiveWalls:  []models.AbsorptionWall{},
			},
		}
	}

	// Read-lock individual stock memory safely without blocking other concurrent readers
	mem.mu.RLock()
	defer mem.mu.RUnlock()

	return mem.State
}

// ReconstituteHQState handles crash-recovery parameters safely on backend initialization
func (h *Headquarters) ReconstituteHQState(ctx context.Context, token uint32, symbol string) {
	if h.pool == nil {
		return
	}
	h.stocksMu.Lock()
	defer h.stocksMu.Unlock()

	var dir string
	var bias, vwp, eff float64

	// Pull previous nested snapshot from the TimescaleDB hypertable block
	query := `
		SELECT 
			COALESCE((hq_intelligence->>'direction'), 'NONE'),
			COALESCE((hq_intelligence->'flow_metrics'->>'bias_score')::DOUBLE PRECISION, 0.0),
			COALESCE((hq_intelligence->'flow_metrics'->>'vwp_delta')::DOUBLE PRECISION, 0.0),
			COALESCE((hq_intelligence->'flow_metrics'->>'efficiency')::DOUBLE PRECISION, 0.0)
		FROM gidh_bars
		WHERE instrument_token = $1
		ORDER BY timestamp DESC LIMIT 1;`

	err := h.pool.QueryRow(ctx, query, token).Scan(&dir, &bias, &vwp, &eff)
	if err != nil {
		return // Fresh startup baseline session state
	}

	h.stocks[token] = &StockMemory{
		TickBuffer: make([]models.TickData, 0, 100000),
		State: models.HqIntelligencePayload{
			Direction: dir,
			FlowMetrics: models.TapeTelemetryUnits{
				BiasScore:  bias,
				VwpDelta:   vwp,
				Efficiency: eff,
			},
		},
	}
}

// ProcessClosedBar runs our absorption validation matrix on a finalized, completed bar instance.
// This prevents real-time tick flickering and guarantees the bubbles stay locked forever on bar close.
func (h *Headquarters) ProcessClosedBar(bar *models.Bar, telemetry models.TapeTelemetryUnits) models.HqIntelligencePayload {
	payload := models.HqIntelligencePayload{
		Direction: "NONE",
		FlowMetrics: models.TapeTelemetryUnits{
			BiasScore:    telemetry.BiasScore,
			VwpDelta:     telemetry.VwpDelta,
			Efficiency:   telemetry.Efficiency,
			IsAbsorption: false,
			ActiveWalls:  []models.AbsorptionWall{},
		},
	}

	// 1. Extract finalized baseline percentile ranks computed inside this bar
	volRank := bar.VolumeRank
	priceRank := bar.PriceRank

	// 2. Define our Two-Tier Reliability Rules
	const lowEfficiencyLimit = 0.05

	// Condition: High volume deployment met with suppressed spatial progress
	if volRank >= 6 && priceRank <= 3 && telemetry.Efficiency <= lowEfficiencyLimit {

		if telemetry.BiasScore >= 0.50 {
			// Aggressive buyers lifting offers were completely absorbed by an institutional ceiling
			payload.Direction = "ABSORPTION_SHORT"
			payload.FlowMetrics.IsAbsorption = true
			payload.FlowMetrics.ActiveWalls = append(payload.FlowMetrics.ActiveWalls, models.AbsorptionWall{
				Direction:       "ABSORPTION_SHORT",
				AbsorptionPrice: bar.High, // Pin bubble to the top wick barrier of the bar
			})

		} else if telemetry.BiasScore <= -0.50 {
			// Aggressive sellers slamming bids were completely absorbed by an institutional floor
			payload.Direction = "ABSORPTION_LONG"
			payload.FlowMetrics.IsAbsorption = true
			payload.FlowMetrics.ActiveWalls = append(payload.FlowMetrics.ActiveWalls, models.AbsorptionWall{
				Direction:       "ABSORPTION_LONG",
				AbsorptionPrice: bar.Low, // Pin bubble to the bottom wick floor of the bar
			})
		}
	}

	// 3. Fallback to standard trends if no institutional wall was identified
	if !payload.FlowMetrics.IsAbsorption {
		if telemetry.BiasScore >= 0.25 {
			payload.Direction = "BULLISH"
		} else if telemetry.BiasScore <= -0.25 {
			payload.Direction = "BEARISH"
		} else {
			payload.Direction = "NONE"
		}
	}

	return payload
}
