package pipeline

import (
	"gidh-backend/internal/service/models"
	"time"
)

// candleState optimizes active high-frequency calculations via map caching.
type candleState struct {
	bar             *models.Bar
	heatmapMap      map[float64]*models.HeatmapCell
	maxAnomalyCount int
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar:        newBar(ts, price, token, name, timeframe),
		heatmapMap: make(map[float64]*models.HeatmapCell),
	}
}

func (bm *BarManager) accumulateMicrostructure(cs *candleState, tick *models.EnrichedTick, tickVol float64) {
	bin := tick.AnomalyBin
	ms := tick.Microstructure

	cell, exists := cs.heatmapMap[bin]
	if !exists {
		cell = &models.HeatmapCell{PriceBin: bin}
		cs.heatmapMap[bin] = cell
	}

	// Increment metrics
	if tick.HasAnomaly {
		cell.AnomalyCount++
		if cell.AnomalyCount > cs.maxAnomalyCount {
			cs.maxAnomalyCount = cell.AnomalyCount
		}
	}

	// Just aggregate the pre-calculated math from AnalyticsStage
	cell.CellVolume += tickVol
	cell.AggressiveBuyVol += ms.AggressiveBuy
	cell.AggressiveSellVol += ms.AggressiveSell
	cell.DepthImbalance = ms.DepthImbalance // You might want a moving average here eventually
	cell.OrderFlowDelta += ms.NormalizedVOFI
	cell.ConsumedBidLiq += ms.ConsumedBid
	cell.ConsumedAskLiq += ms.ConsumedAsk
	cell.ReplenishmentBid += ms.ReplenishmentBid
	cell.ReplenishmentAsk += ms.ReplenishmentAsk
	cell.MicroPrice = ms.MicroPrice

	// Dynamic resizing
	if cs.maxAnomalyCount > 0 {
		cell.IntensityScore = float64(cell.AnomalyCount) / float64(cs.maxAnomalyCount)
	}
}

// Convert backend cells to lightweight UI cells
func (cs *candleState) finalizeTransforms() *models.Bar {
	uiCells := make([]models.UIHeatmapCell, 0)

	for _, cell := range cs.heatmapMap {
		if cell.CellVolume <= 0 { // Filter out "ghost" updates
			continue
		}

		uiCells = append(uiCells, models.UIHeatmapCell{
			P: cell.PriceBin,
			V: cell.CellVolume,
			I: cell.IntensityScore,
			O: cell.OrderFlowDelta,
		})
	}

	cs.bar.Heatmap = uiCells
	return cs.bar
}

func newBar(ts time.Time, price float64, token uint32, name string, timeframe string) *models.Bar {
	var truncatedTs time.Time
	switch timeframe {
	case "5m":
		truncatedTs = ts.Truncate(5 * time.Minute)
	case "3m":
		truncatedTs = ts.Truncate(3 * time.Minute)
	default:
		truncatedTs = ts.Truncate(time.Minute)
	}

	return &models.Bar{
		Timestamp:       truncatedTs,
		InstrumentToken: int32(token),
		StockName:       name,
		Timeframe:       timeframe,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
		Volume:          0,
		Ticks:           make([]models.TickData, 0, 60),
	}
}

func updateBar(b *models.Bar, price, vol float64) {
	if price > b.High {
		b.High = price
	}
	if price < b.Low {
		b.Low = price
	}
	b.Close = price
	b.Volume += vol
}

func (bm *BarManager) updateTimeframe(stateMap map[uint32]*candleState, token uint32, ts time.Time, price, vol float64, duration time.Duration, timeframe string, tick *models.EnrichedTick) {
	cs := stateMap[token]
	expectedTs := ts.Truncate(duration)

	// Roll candle over if timeframe boundary milestone is breached
	if expectedTs.After(cs.bar.Timestamp) {
		finalBar := cs.finalizeTransforms()
		if bm.writer != nil {
			bm.writer.AddBar(*finalBar)
		}
		stateMap[token] = newCandleState(ts, price, token, tick.Raw.StockName, timeframe)
		cs = stateMap[token]
	}

	if !expectedTs.Before(cs.bar.Timestamp) {
		updateBar(cs.bar, price, vol)
		cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		cs.bar.VWAP = tick.Raw.AverageTradedPrice

		if timeframe == "1m" {
			cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
		}
		if tick.VolProfile != nil {
			cs.bar.POC = tick.VolProfile.POC
			cs.bar.VAH = tick.VolProfile.VAH
			cs.bar.VAL = tick.VolProfile.VAL
		}

		// 🔥 Process microstructure footprints under an O(1) layout structure
		bm.accumulateMicrostructure(cs, tick, vol)

		// Broadcast rolling live frames down WebSocket pipes
		if bm.wsHub != nil {
			bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{"type": "bar", "data": cs.finalizeTransforms()})
		}
	}
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.state1m = make(map[uint32]*candleState)
	bm.state3m = make(map[uint32]*candleState)
	bm.state5m = make(map[uint32]*candleState)
	bm.lastTickState = make(map[uint32]*tokenTickState)
}

func (cs *candleState) finalizeTransformsForUI() []models.UIHeatmapCell {
	uiCells := make([]models.UIHeatmapCell, 0)

	for _, cell := range cs.heatmapMap {
		// 1. Filter out quote-only "ghost" cells
		if cell.CellVolume <= 0 {
			continue
		}

		// 2. Map to the simplified UI struct
		uiCells = append(uiCells, models.UIHeatmapCell{
			P: cell.PriceBin,
			V: cell.CellVolume,
			I: cell.IntensityScore,
			O: cell.OrderFlowDelta,
		})
	}
	return uiCells
}
