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
	macroQueue []macroPoint
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar:        newBar(ts, price, token, name, timeframe),
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

	// 1. Maintain OHLC Bounds
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// 2. Aggregate Core Root Level Fields
	cs.bar.Volume += vol
	cs.bar.TickCount++
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	// 3. Simple Dynamic Net Change Percentage for UI
	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	} else {
		cs.bar.ChangePct = 0.0
	}

	// 4. 🔥 PEAK TRACKING FOR STATISTICAL METRICS (Option A)
	// Track the absolute maximum tick frequency burst seen during this candle window
	if tick.Enrichment.TickZ > cs.bar.Metrics.MaxTickCountZ {
		cs.bar.Metrics.MaxTickCountZ = tick.Enrichment.TickZ
	}

	// Track the highest absolute value for VolumeZ (captures both extreme selling/buying volume spikes)
	if math.Abs(tick.Enrichment.VolumeZ) > math.Abs(cs.bar.Metrics.VolumeZ) {
		cs.bar.Metrics.VolumeZ = tick.Enrichment.VolumeZ
	}

	// Track the highest absolute value for TickZ
	if math.Abs(tick.Enrichment.TickZ) > math.Abs(cs.bar.Metrics.TickZ) {
		cs.bar.Metrics.TickZ = tick.Enrichment.TickZ
	}

	// Keep the highest non-Gaussian percentile tier reached during the candle lifetime (e.g., lock in P99 or P95)
	if tick.Enrichment.RangePercentile == "P99" ||
		(tick.Enrichment.RangePercentile == "P95" && cs.bar.Metrics.RangePercentile != "P99") {
		cs.bar.Metrics.RangePercentile = tick.Enrichment.RangePercentile
	} else if cs.bar.Metrics.RangePercentile == "" {
		cs.bar.Metrics.RangePercentile = "NORMAL"
	}

	// Keep the highest non-Gaussian efficiency percentile tier reached
	if tick.Enrichment.EfficiencyPercentile == "P99" ||
		(tick.Enrichment.EfficiencyPercentile == "P95" && cs.bar.Metrics.EfficiencyPercentile != "P99") {
		cs.bar.Metrics.EfficiencyPercentile = tick.Enrichment.EfficiencyPercentile
	} else if cs.bar.Metrics.EfficiencyPercentile == "" {
		cs.bar.Metrics.EfficiencyPercentile = "NORMAL"
	}

	// 5. Hydrate Auction Market Theory Matrix
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	// Ingest ticks for 1m granular buffers
	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.state1m = make(map[uint32]*candleState)
	bm.state3m = make(map[uint32]*candleState)
	bm.state5m = make(map[uint32]*candleState)
	bm.state10m = make(map[uint32]*candleState)
	bm.state15m = make(map[uint32]*candleState)
}
