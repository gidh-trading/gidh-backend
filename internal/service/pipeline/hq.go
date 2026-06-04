// internal/service/pipeline/hq.go
package pipeline

import (
	"context"
	"sync"

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
		lookback = 300
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
			},
		}
		h.stocks[token] = mem
	}
	h.stocksMu.Unlock()

	mem.mu.Lock()
	defer mem.mu.Unlock()

	// Append raw tick to whole-day memory buffer
	mem.TickBuffer = append(mem.TickBuffer, tick.Raw)

	bufferSize := len(mem.TickBuffer)
	if bufferSize < h.lookbackTicks {
		return
	}

	// Compute un-spoofable committed metrics
	sample := mem.TickBuffer[bufferSize-h.lookbackTicks : bufferSize]
	var cumulativeVwpDelta float64 = 0
	var totalExecutedVolume int64 = 0
	highestPrice := sample[0].LastPrice
	lowestPrice := sample[0].LastPrice

	for i := 1; i < len(sample); i++ {
		prev := sample[i-1]
		curr := sample[i]

		priceDelta := curr.LastPrice - prev.LastPrice
		tickVol := curr.CumulativeVolume - prev.CumulativeVolume
		if tickVol < 0 {
			tickVol = curr.LastTradedQuantity
		}

		cumulativeVwpDelta += float64(tickVol) * priceDelta
		totalExecutedVolume += tickVol

		if curr.LastPrice > highestPrice {
			highestPrice = curr.LastPrice
		}
		if curr.LastPrice < lowestPrice {
			lowestPrice = curr.LastPrice
		}
	}

	// Save immediate metrics directly into the local state snapshot
	mem.State.LiveMetrics.VwpDelta = cumulativeVwpDelta
	if cumulativeVwpDelta > 0 {
		mem.State.Direction = "BULLISH"
	} else if cumulativeVwpDelta < 0 {
		mem.State.Direction = "BEARISH"
	} else {
		mem.State.Direction = "NONE"
	}

	priceSpan := highestPrice - lowestPrice
	if totalExecutedVolume > 0 {
		mem.State.LiveMetrics.Efficiency = priceSpan / float64(totalExecutedVolume)
	} else {
		mem.State.LiveMetrics.Efficiency = 0
	}
}

// GetIntelligenceSnapshot is the high-speed interface for BarManager to read metrics safely
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

// ReconstituteHQState handles crash recoveries safely on boot
func (h *Headquarters) ReconstituteHQState(ctx context.Context, token uint32, symbol string) {
	if h.pool == nil {
		return
	}
	h.stocksMu.Lock()
	defer h.stocksMu.Unlock()

	var dir string
	var vwp, eff float64

	// Read from the hypertable's latest snapshot json block data field
	query := `
		SELECT 
			COALESCE((hq_intelligence->>'direction'), 'NONE'),
			COALESCE((hq_intelligence->'live_metrics'->>'vwp_delta')::DOUBLE PRECISION, 0.0),
			COALESCE((hq_intelligence->'live_metrics'->>'efficiency')::DOUBLE PRECISION, 0.0)
		FROM gidh_bars
		WHERE instrument_token = $1
		ORDER BY timestamp DESC LIMIT 1;`

	err := h.pool.QueryRow(ctx, query, token).Scan(&dir, &vwp, &eff)
	if err != nil {
		return // Fresh startup baseline
	}

	h.stocks[token] = &StockMemory{
		TickBuffer: make([]models.TickData, 0, 100000),
		State: models.HqIntelligencePayload{
			Direction: dir,
			LiveMetrics: models.HqLiveMetricsUnits{
				VwpDelta:   vwp,
				Efficiency: eff,
			},
		},
	}
}
