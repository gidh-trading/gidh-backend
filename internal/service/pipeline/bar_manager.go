// internal/service/pipeline/bar_manager.go

package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type BarManager struct {
	loc    *time.Location
	bar1m  map[uint32]*models.Bar
	bar3m  map[uint32]*models.Bar
	bar5m  map[uint32]*models.Bar
	mu     sync.RWMutex
	writer *writer.DBWriter
	wsHub  *ws.Hub
}

func NewBarManager(w *writer.DBWriter, hub *ws.Hub) *BarManager {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return &BarManager{
		loc:    loc,
		bar1m:  make(map[uint32]*models.Bar),
		bar3m:  make(map[uint32]*models.Bar),
		bar5m:  make(map[uint32]*models.Bar),
		writer: w,
		wsHub:  hub,
	}
}

func (bm *BarManager) Process(tick *models.EnrichedTick) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(bm.loc)
	name := tick.Raw.StockName

	// Ensure structural instances are present across timeframes
	if bm.bar1m[token] == nil {
		bm.bar1m[token] = newBar(ts, price, token, name, "1m")
	}
	if bm.bar3m[token] == nil {
		bm.bar3m[token] = newBar(ts, price, token, name, "3m")
	}
	if bm.bar5m[token] == nil {
		bm.bar5m[token] = newBar(ts, price, token, name, "5m")
	}

	// Process individual time horizons
	bm.updateTimeframe(bm.bar1m, token, ts, price, vol, time.Minute, "1m", tick)
	bm.updateTimeframe(bm.bar3m, token, ts, price, vol, 3*time.Minute, "3m", tick)
	bm.updateTimeframe(bm.bar5m, token, ts, price, vol, 5*time.Minute, "5m", tick)

	return nil
}

func (bm *BarManager) updateTimeframe(barMap map[uint32]*models.Bar, token uint32, ts time.Time, price, vol float64, duration time.Duration, timeframe string, tick *models.EnrichedTick) {
	b := barMap[token]
	expectedTs := ts.Truncate(duration)

	// Roll candle over if boundary milestone is breached
	if expectedTs.After(b.Timestamp) {
		if bm.writer != nil {
			bm.writer.AddBar(*b)
		}
		barMap[token] = newBar(ts, price, token, tick.Raw.StockName, timeframe)
		b = barMap[token]
	}

	if !expectedTs.Before(b.Timestamp) {
		// General aggregation updates
		updateBar(b, price, vol)
		b.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b.VWAP = tick.Raw.AverageTradedPrice

		if timeframe == "1m" {
			b.Ticks = append(b.Ticks, tick.Raw)
		}
		if tick.VolProfile != nil {
			b.POC = tick.VolProfile.POC
			b.VAH = tick.VolProfile.VAH
			b.VAL = tick.VolProfile.VAL
		}

		// 🔥 Accumulate microstructural heatmap anomalies into the passive carrier
		if tick.HasAnomaly {
			bm.accumulateHeatmap(b, tick.AnomalyBin)
		}

		// Broadcast real-time canvas data
		if bm.wsHub != nil {
			bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{"type": "bar", "data": b})
		}
	}
}

func (bm *BarManager) accumulateHeatmap(b *models.Bar, bin float64) {
	found := false
	for i := range b.Heatmap {
		if b.Heatmap[i].PriceBin == bin {
			b.Heatmap[i].AnomalyCount++
			found = true
			break
		}
	}

	if !found {
		b.Heatmap = append(b.Heatmap, models.HeatmapCell{
			PriceBin:       bin,
			AnomalyCount:   1,
			IntensityScore: 0.0,
		})
	}

	// Dynamic relative rescaling for clear alpha channel visualizations
	maxCount := 0
	for _, cell := range b.Heatmap {
		if cell.AnomalyCount > maxCount {
			maxCount = cell.AnomalyCount
		}
	}

	if maxCount > 0 {
		for i := range b.Heatmap {
			b.Heatmap[i].IntensityScore = float64(b.Heatmap[i].AnomalyCount) / float64(maxCount)
		}
	}
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.bar1m = make(map[uint32]*models.Bar)
	bm.bar3m = make(map[uint32]*models.Bar)
	bm.bar5m = make(map[uint32]*models.Bar)
}
