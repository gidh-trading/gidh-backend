// Update internal/service/pipeline/helpers.go completely with these functions and structs

package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
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

func (bm *BarManager) processTickForCandle(cs *candleState, tick *models.EnrichedTick, vol float64, timeframe string) {
	price := tick.Raw.LastPrice

	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	cs.bar.Volume += vol
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

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
}

func (bm *BarManager) accumulateMicrostructure(cs *candleState, tick *models.EnrichedTick, tickVol float64) {
	if !tick.HasAnomaly {
		return
	}

	bin := tick.AnomalyBin

	cell, exists := cs.heatmapMap[bin]
	if !exists {
		cell = &models.HeatmapCell{PriceBin: bin}
		cs.heatmapMap[bin] = cell
	}

	cell.CellVolume += tickVol
	cell.AggressiveBuy += tick.Microstructure.AggressiveBuy
	cell.AggressiveSell += tick.Microstructure.AggressiveSell
	cell.IntensityScore += tickVol * tick.RelativeVolume

	// 👈 NEW: Track peak execution metrics for historical DNA identification
	if tick.VolumeZ > cell.MaxVolumeZ {
		cell.MaxVolumeZ = tick.VolumeZ
	}
	if tick.TickCountZ > cell.MaxTickZ {
		cell.MaxTickZ = tick.TickCountZ
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

func (cs *candleState) finalizeSlopesForUI() models.TrendSlopes {
	var latestOffset int = -1
	for o := range cs.mpMap {
		if o > latestOffset {
			latestOffset = o
		}
	}

	if latestOffset == -1 {
		return models.TrendSlopes{}
	}

	normalize := func(val, maxVal float64) float64 {
		if maxVal == 0 {
			return 0
		}
		intensity := math.Abs(val) / maxVal
		if intensity < 0.1 {
			intensity = 0.1
		}
		if val < 0 {
			return -intensity
		}
		return intensity
	}

	return models.TrendSlopes{
		Price:  normalize(cs.mpMap[latestOffset], cs.maxMp),
		VWAP:   normalize(cs.mvMap[latestOffset], cs.maxMv),
		Volume: normalize(cs.mvolMap[latestOffset], cs.maxMvol),
	}
}
