package pipeline

import (
	"fmt"
	"gidh-backend/internal/service/models"
	"math"
	"sort"
	"time"
)

type macroPoint struct {
	x      float64
	price  float64
	vwap   float64
	volume float64
}

type candleState struct {
	bar        *models.Bar
	heatmapMap map[float64]*models.HeatmapCell

	// Slope Maps keyed by offset
	mpMap   map[int]float64
	mvMap   map[int]float64
	mvolMap map[int]float64

	// Track the max magnitudes for opacity calculation
	maxMp   float64
	maxMv   float64
	maxMvol float64

	// MACRO ROLLING STATE (Tracks the last 10 closed bars)
	macroQueue []macroPoint
	PriceReg   RollingRegression
	VWAPReg    RollingRegression
	VolReg     RollingRegression

	lastBroadcast time.Time
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar:        newBar(ts, price, token, name, timeframe),
		heatmapMap: make(map[float64]*models.HeatmapCell),

		mpMap:   make(map[int]float64),
		mvMap:   make(map[int]float64),
		mvolMap: make(map[int]float64),

		macroQueue: make([]macroPoint, 0, 10),
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

	// Track Stealth Iceberg Activity
	cs.bar.TickCount++
	if tick.TickCountZ > cs.bar.MaxTickCountZ {
		cs.bar.MaxTickCountZ = tick.TickCountZ
	}

	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}

	// ----------------------------------------------------
	// 🔥 ATTACH SLOPES AND TRACK MAGNITUDES
	// ----------------------------------------------------
	offset := int(tick.Raw.Timestamp.Sub(cs.bar.Timestamp).Seconds())

	cs.mpMap[offset] = tick.MicroPriceSlope
	if absMp := math.Abs(tick.MicroPriceSlope); absMp > cs.maxMp {
		cs.maxMp = absMp
	}

	cs.mvMap[offset] = tick.MicroVWAPSlope
	if absMv := math.Abs(tick.MicroVWAPSlope); absMv > cs.maxMv {
		cs.maxMv = absMv
	}

	cs.mvolMap[offset] = tick.MicroVolumeSlope
	if absMvol := math.Abs(tick.MicroVolumeSlope); absMvol > cs.maxMvol {
		cs.maxMvol = absMvol
	}

	bm.accumulateMicrostructure(cs, tick, vol)

	// Broadcast rolling live frames down WebSocket pipes
	if bm.wsHub != nil && time.Since(cs.lastBroadcast) > 250*time.Millisecond {
		cs.bar.Heatmap = cs.finalizeTransformsForUI()
		cs.bar.Slopes = cs.finalizeSlopesForUI()

		bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{
			"type": "bar",
			"data": cs.bar,
		})
		cs.lastBroadcast = time.Now()
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

		// Calculate the net direction for this price bucket
		tradeDelta := cell.AggressiveBuy - cell.AggressiveSell

		uiCells = append(uiCells, models.UIHeatmapCell{
			P: cell.PriceBin,
			V: cell.CellVolume,
			D: tradeDelta,
			I: cell.IntensityScore,
		})
	}
	return uiCells
}

// Helper function to build the render arrays
func buildRenderCells(dataMap map[int]float64, maxVal float64) []models.SlopeRenderCell {
	cells := make([]models.SlopeRenderCell, 0, len(dataMap))

	for o, val := range dataMap {
		// 1. Calculate Opacity based on magnitude
		opacity := 0.1 // Minimum baseline visibility
		if maxVal > 0 {
			opacity = math.Abs(val) / maxVal
			if opacity < 0.1 {
				opacity = 0.1
			}
		}

		// 2. Determine Color Polarity
		r, g, b := 255, 0, 0 // Default to Red (Negative)
		if val > 0 {
			r, g, b = 0, 255, 0 // Change to Green (Positive)
		}

		cells = append(cells, models.SlopeRenderCell{
			O:     o,
			V:     val,
			Color: fmt.Sprintf("rgba(%d, %d, %d, %.2f)", r, g, b, opacity),
		})
	}

	// 3. Sort chronologically so UI gets a clean timeline
	sort.Slice(cells, func(i, j int) bool { return cells[i].O < cells[j].O })

	return cells
}

func (cs *candleState) finalizeSlopesForUI() models.TrendSlopes {
	// Since all 3 maps are populated simultaneously by offset, we just use mpMap's length
	history := make([][]float64, 0, len(cs.mpMap))

	// Get sorted keys for a deterministic timeline
	offsets := make([]int, 0, len(cs.mpMap))
	for o := range cs.mpMap {
		offsets = append(offsets, o)
	}
	sort.Ints(offsets)

	// Helper to normalize magnitude + polarity into a single float (-1.0 to 1.0)
	normalize := func(val, maxVal float64) float64 {
		if maxVal == 0 {
			return 0
		}
		intensity := math.Abs(val) / maxVal
		if intensity < 0.1 {
			intensity = 0.1
		} // baseline visibility
		if val < 0 {
			return -intensity
		} // negative = red
		return intensity // positive = green
	}

	for _, o := range offsets {
		history = append(history, []float64{
			float64(o),
			normalize(cs.mpMap[o], cs.maxMp),
			normalize(cs.mvMap[o], cs.maxMv),
			normalize(cs.mvolMap[o], cs.maxMvol),
		})
	}

	return models.TrendSlopes{History: history}
}
