package pipeline

import (
	"gidh-backend/internal/service/models"
	"sort"
	"time"
)

// candleState optimizes active high-frequency calculations via map caching.
type candleState struct {
	bar        *models.Bar
	heatmapMap map[float64]*models.HeatmapCell
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar:        newBar(ts, price, token, name, timeframe),
		heatmapMap: make(map[float64]*models.HeatmapCell),
	}
}

func newBar(ts time.Time, price float64, token uint32, name, timeframe string) *models.Bar {
	return &models.Bar{
		Timestamp:       ts,
		InstrumentToken: int32(token),
		StockName:       name,
		Timeframe:       timeframe,
		Open:            price,
		High:            price,
		Low:             price,
		Close:           price,
		Ticks:           make([]models.TickData, 0),
	}
}

// processTickForCandle updates the running OHLCV and heatmap data for a specific timeframe candle
func (bm *BarManager) processTickForCandle(cs *candleState, tick *models.EnrichedTick, vol float64, timeframe string) {
	price := tick.Raw.LastPrice

	// Update OHLC
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// Update Volume & Auction Data
	cs.bar.Volume += vol
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	// 🕵️ NEW: Track Stealth Iceberg Activity
	cs.bar.TickCount++
	if tick.TickCountZ > cs.bar.MaxTickCountZ {
		cs.bar.MaxTickCountZ = tick.TickCountZ
	}

	// Map the calculated Volume Profile metrics to the Bar
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	// Store raw ticks only for the lowest timeframe if needed for historical replay
	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}

	// 🔥 Process microstructure footprints (Only processes anomalous ticks)
	bm.accumulateMicrostructure(cs, tick, vol)

	// Broadcast rolling live frames down WebSocket pipes
	if bm.wsHub != nil {
		cs.bar.Heatmap = cs.finalizeTransformsForUI()
		bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{
			"type": "bar",
			"data": cs.bar,
		})
	}
}

// accumulateMicrostructure builds the institutional footprint map
func (bm *BarManager) accumulateMicrostructure(cs *candleState, tick *models.EnrichedTick, tickVol float64) {
	// 1. EARLY EXIT: If it's not an anomaly, drop it completely.
	// Do not create a cell, do not track the volume.
	if !tick.HasAnomaly {
		return
	}

	bin := tick.AnomalyBin

	// 2. Get or create the bucket cell for this price
	cell, exists := cs.heatmapMap[bin]
	if !exists {
		cell = &models.HeatmapCell{PriceBin: bin}
		cs.heatmapMap[bin] = cell
	}

	// 3. Add the Buy/Sell Volume to the bucket
	cell.CellVolume += tickVol
	cell.AggressiveBuy += tick.Microstructure.AggressiveBuy
	cell.AggressiveSell += tick.Microstructure.AggressiveSell

	// 4. Volume-Weighted Intensity Score
	// Multiplies the physical volume by the RVol multiplier.
	// This ensures massive whale drops glow heavily on the UI, not just repeated small ticks.
	cell.IntensityScore += tickVol * tick.RelativeVolume
}

// ClearState resets the BarManager maps (called on session reset or date change)
func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.state1m = make(map[uint32]*candleState)
	bm.state3m = make(map[uint32]*candleState)
	bm.state5m = make(map[uint32]*candleState)
	bm.lastTickState = make(map[uint32]*tokenTickState)
}

// finalizeTransformsForUI converts backend cells to lightweight UI cells
func (cs *candleState) finalizeTransformsForUI() []models.UIHeatmapCell {
	uiCells := make([]models.UIHeatmapCell, 0)

	for _, cell := range cs.heatmapMap {
		if cell.CellVolume <= 0 {
			continue
		}

		tradeDelta := cell.AggressiveBuy - cell.AggressiveSell

		uiCells = append(uiCells, models.UIHeatmapCell{
			P: cell.PriceBin,
			V: cell.CellVolume,
			D: tradeDelta,
			I: cell.IntensityScore,
		})
	}

	// 1. Sort the cells by Intensity (Highest to Lowest)
	sort.Slice(uiCells, func(i, j int) bool {
		return uiCells[i].I > uiCells[j].I
	})

	// 2. THE ABSOLUTE WINNER LOGIC
	if len(uiCells) > 0 {
		winner := uiCells[0]

		// Optional Safety Check: Ensure the "winner" actually has significant intensity.
		// This prevents marking a winner in a dead candle where the highest volume was still tiny.
		// if winner.I < YOUR_MINIMUM_THRESHOLD {
		//     return []models.UIHeatmapCell{}
		// }

		// Return ONLY the absolute dominant level to the UI
		return []models.UIHeatmapCell{winner}
	}

	return uiCells
}
