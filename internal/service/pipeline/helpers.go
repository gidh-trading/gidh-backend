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

// getPercentileRank normalizes non-linear distribution spaces into a linear 1-7 coordinate grid
func getPercentileRank(p string) int {
	switch p {
	case "P99":
		return 7
	case "P95":
		return 6
	case "P90":
		return 5
	case "P50":
		return 4
	case "NORMAL":
		return 3
	case "P10":
		return 2
	case "P05":
		return 1
	default:
		return 3 // Default fallback to mid-tier stationary state
	}
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

	// 1. Structural OHLC Boundary Management
	if price > cs.bar.High {
		cs.bar.High = price
	}
	if price < cs.bar.Low {
		cs.bar.Low = price
	}
	cs.bar.Close = price

	// 2. Direct Aggregate Summaries
	cs.bar.Volume += vol
	cs.bar.TickCount++
	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	// Relative Intraday Net Move Track
	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	// 3. CONTINUOUS LIVE TELEMETRY CRADLE DEFAULTS
	if cs.bar.Metrics.PeakRangePct == "" {
		cs.bar.Metrics.PeakRangePct = "NORMAL"
		cs.bar.Metrics.PeakRangeRank = 3
	}
	if cs.bar.Metrics.PeakEfficiencyPct == "" {
		cs.bar.Metrics.PeakEfficiencyPct = "NORMAL"
	}

	// 4. INTRABAR PEAK ANOMALY EXTRACTION (State Envelope Retention)
	if tick.Enrichment.TickZ > cs.bar.Metrics.MaxTickCountZ {
		cs.bar.Metrics.MaxTickCountZ = tick.Enrichment.TickZ
	}
	if math.Abs(tick.Enrichment.VolumeZ) > math.Abs(cs.bar.Metrics.VolumeZ) {
		cs.bar.Metrics.VolumeZ = tick.Enrichment.VolumeZ
	}
	if math.Abs(tick.Enrichment.TickZ) > math.Abs(cs.bar.Metrics.TickZ) {
		cs.bar.Metrics.TickZ = tick.Enrichment.TickZ
	}
	if tick.Telemetry.Efficiency > cs.bar.Metrics.Efficiency {
		cs.bar.Metrics.Efficiency = tick.Telemetry.Efficiency
	}

	// Legacy fallback sync
	if getPercentileRank(tick.Enrichment.RangePercentile) > getPercentileRank(cs.bar.Metrics.RangePercentile) {
		cs.bar.Metrics.RangePercentile = tick.Enrichment.RangePercentile
	}

	// ------------------------------------------------------------------------
	// VISUALIZATION LAYER COMPRESSION ENGINE
	// ------------------------------------------------------------------------

	// A. Capture Peak Participation Anomaly Magnitude (Statistical Neutrality)
	absVolZ := math.Abs(tick.Enrichment.VolumeZ)
	if absVolZ > cs.bar.Metrics.AbsVolumeZ {
		cs.bar.Metrics.AbsVolumeZ = absVolZ
	}

	// B. Track Response Excursion Extremes (Enforces UI Topology Stability)
	currentRangeRank := getPercentileRank(tick.Enrichment.RangePercentile)

	// We check for priority rank dominance. For anomoloies, we want to log the
	// most severe expansion (P95/P99) OR severe compressions (P05/P10).
	// If the current bar state is still "NORMAL" (Rank 3), any deviation takes precedence.
	if cs.bar.Metrics.PeakRangeRank == 3 {
		cs.bar.Metrics.PeakRangeRank = currentRangeRank
		cs.bar.Metrics.PeakRangePct = tick.Enrichment.RangePercentile
	} else {
		// If we are tracking an anomaly, prioritize highest statistical extremity.
		// For expansions (Ranks 5,6,7), higher rank wins.
		// For compressions (Ranks 1,2), lower rank represents a higher structural anomaly.
		if currentRangeRank > 4 && currentRangeRank > cs.bar.Metrics.PeakRangeRank {
			cs.bar.Metrics.PeakRangeRank = currentRangeRank
			cs.bar.Metrics.PeakRangePct = tick.Enrichment.RangePercentile
		} else if currentRangeRank < 3 && currentRangeRank < cs.bar.Metrics.PeakRangeRank {
			cs.bar.Metrics.PeakRangeRank = currentRangeRank
			cs.bar.Metrics.PeakRangePct = tick.Enrichment.RangePercentile
		}
	}

	// C. Capture Peak Efficiency Anomaly (Bar Chart Target)
	currentEffRank := getPercentileRank(tick.Enrichment.EfficiencyPercentile)
	targetEffRank := getPercentileRank(cs.bar.Metrics.PeakEfficiencyPct)

	if currentEffRank > 4 && currentEffRank > targetEffRank {
		cs.bar.Metrics.PeakEfficiencyPct = tick.Enrichment.EfficiencyPercentile
	} else if currentEffRank < 3 && currentEffRank < targetEffRank {
		cs.bar.Metrics.PeakEfficiencyPct = tick.Enrichment.EfficiencyPercentile
	}

	// 5. Build Market Auction Framework Layout
	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

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
