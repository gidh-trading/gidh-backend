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

// internal/service/pipeline/helpers.go

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

	// 📈 CALCULATE & ASSIGN REAL-TIME CHANGE PERCENTAGE FOR UI
	// Zerodha passes the net change amount in tick.Raw.Change.
	// Previous Close = LastPrice - Change.
	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	} else {
		cs.bar.ChangePct = 0.0
	}

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
	bm.state10m = make(map[uint32]*candleState) // NEW
	bm.state15m = make(map[uint32]*candleState) // NEW
	bm.lastTickState = make(map[uint32]*tokenTickState)
}

// internal/service/pipeline/helpers.go

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

	// 1. Fetch raw, unaltered current slopes from your linear regression buffers
	rawPriceSlope := cs.mpMap[latestOffset]
	rawVwapSlope := cs.mvMap[latestOffset]
	rawVolSlope := cs.mvolMap[latestOffset]

	// 2. Extract statistical metrics across the current candle's microstructure history
	priceMean, priceStdDev := calculateSlopeStats(cs.mpMap)

	// 3. Compute the Statistical Z-Score for Price Slope
	priceZScore := 0.0
	if priceStdDev > 0 {
		priceZScore = (rawPriceSlope - priceMean) / priceStdDev
	}

	// 4. Map the Z-Score to a clean Alpha Density (0.0 to 1.0)
	// In statistics, a Z-score of 2.0 to 2.5 means an extreme velocity shift.
	// We divide the absolute Z-score by a Target Threshold (e.g., 2.5) so that
	// any highly unusual statistical movement results in an alpha near 1.0 (pure solid color).
	absPriceZ := math.Abs(priceZScore)
	statisticalAlpha := absPriceZ / 2.5

	// Safe bounding limits to keep HTML5 Canvas alpha arguments from breaking
	if statisticalAlpha > 0.95 {
		statisticalAlpha = 0.95
	}
	if statisticalAlpha < 0.10 && absPriceZ > 0.2 {
		statisticalAlpha = 0.15 // Solid base visibility floor for mild activity
	}

	return models.TrendSlopes{
		Price:          rawPriceSlope, // Transmitting raw slope over the wire for indicators
		VWAP:           rawVwapSlope,
		Volume:         rawVolSlope,
		PriceIntensity: statisticalAlpha, // 👈 Handing the UI a statistically verified alpha value
	}
}

func calculateSlopeStats(slopeMap map[int]float64) (mean float64, stdDev float64) {
	if len(slopeMap) == 0 {
		return 0, 0
	}

	var sum float64
	for _, val := range slopeMap {
		sum += val
	}
	mean = sum / float64(len(slopeMap))

	var varianceSum float64
	for _, val := range slopeMap {
		diff := val - mean
		varianceSum += diff * diff
	}

	variance := varianceSum / float64(len(slopeMap))
	stdDev = math.Sqrt(variance)
	return mean, stdDev
}
