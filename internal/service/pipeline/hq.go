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
	mu         sync.RWMutex
	State      models.HqIntelligencePayload
	TickBuffer []models.TickData
}

type Headquarters struct {
	pool          *pgxpool.Pool
	positionCtx   order.PositionManager
	stocks        map[uint32]*StockMemory
	stocksMu      sync.RWMutex
	lookbackTicks int
}

func NewHeadquarters(pool *pgxpool.Pool, pm order.PositionManager, lookback int) *Headquarters {
	if lookback <= 0 {
		lookback = 300 // Tracks continuous tape dynamics across a baseline 300-tick window
	}
	return &Headquarters{
		pool:          pool,
		positionCtx:   pm,
		stocks:        make(map[uint32]*StockMemory),
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
					BiasScore:  0.0,
					VwpDelta:   0.0,
					Efficiency: 0.0,
				},
			},
		}
		h.stocks[token] = mem
	}
	h.stocksMu.Unlock()

	mem.mu.Lock()
	defer mem.mu.Unlock()

	// Append raw tick to session-continuous rolling slice matrix buffer
	mem.TickBuffer = append(mem.TickBuffer, tick.Raw)

	bufferSize := len(mem.TickBuffer)
	if bufferSize < h.lookbackTicks {
		return
	}

	// Isolate lookback window slice bounds
	sample := mem.TickBuffer[bufferSize-h.lookbackTicks : bufferSize]

	// Delegate continuous mathematical processing to pure routine functions
	telemetry := analytics.CalculateTapeTelemetry(sample)

	// Persist to internal structural state cache blocks
	mem.State.FlowMetrics = telemetry

	// Quantize categorical descriptive boundaries for backward-compatible terminal pipelines
	if telemetry.BiasScore >= 0.25 {
		mem.State.Direction = "BULLISH"
	} else if telemetry.BiasScore <= -0.25 {
		mem.State.Direction = "BEARISH"
	} else {
		mem.State.Direction = "NONE"
	}
}

// GetIntelligenceSnapshot is the high-speed concurrent interface for BarManager mapping
func (h *Headquarters) GetIntelligenceSnapshot(token uint32) models.HqIntelligencePayload {
	h.stocksMu.RLock()
	mem, exists := h.stocks[token]
	h.stocksMu.RUnlock()

	if !exists || mem == nil {
		return models.HqIntelligencePayload{Direction: "NONE"}
	}

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
