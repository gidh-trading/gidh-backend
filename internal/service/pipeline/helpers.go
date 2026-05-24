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
	case "P97":
		return 7 // burst/extreme
	case "P90":
		return 6 // elevated
	case "P75":
		return 5 // active
	case "P50":
		return 4 // baseline
	case "P25":
		return 3 // below normal
	case "P10":
		return 2 // weak
	case "P05":
		return 1 // drought
	default:
		return 4 // Balanced baseline fallback if an unexpected string leaks in
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

	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	// 3. CONTINUOUS LIVE TELEMETRY DEFAULT INITIALIZATION
	if cs.bar.Metrics.PeakVolumeZRank == 0 {
		cs.bar.Metrics.PeakVolumeZRank = 4
	}
	if cs.bar.Metrics.PeakPriceRank == 0 {
		cs.bar.Metrics.PeakPriceRank = 4
	}
	if cs.bar.Metrics.PeakTickRank == 0 {
		cs.bar.Metrics.PeakTickRank = 4
	}

	// ------------------------------------------------------------------------
	// TRINARY DIRECTIONAL ANOMALY ENGINE
	// ------------------------------------------------------------------------

	// A. Continuously calculate the underlying visual metrics for the current tick state
	windowRange := tick.Telemetry.RealizedRange
	if windowRange > 0 {
		displacement := (cs.bar.Close - cs.bar.Open) / windowRange
		// Clamp displacement between -1.0 and 1.0
		if displacement > 1.0 {
			displacement = 1.0
		} else if displacement < -1.0 {
			displacement = -1.0
		}
		cs.bar.Metrics.NormalizedDisplacement = displacement
	}

	// B. Calculate Wick Asymmetry: Positive = Buying Tail, Negative = Selling Wick
	highLowRange := cs.bar.High - cs.bar.Low
	if highLowRange > 0 {
		maxOpenClose := cs.bar.Open
		if cs.bar.Close > maxOpenClose {
			maxOpenClose = cs.bar.Close
		}
		minOpenClose := cs.bar.Open
		if cs.bar.Close < minOpenClose {
			minOpenClose = cs.bar.Close
		}

		upperWick := cs.bar.High - maxOpenClose
		lowerWick := minOpenClose - cs.bar.Low

		// Normalized asymmetry metric scaled against the total candle range
		cs.bar.Metrics.WickAsymmetry = (lowerWick - upperWick) / highLowRange
	}

	// C. Evaluation Gate: Check if current tick's Absolute Volume hits your P90 or P97 baseline
	currentVolZRank := getPercentileRank(tick.Enrichment.VolumeZPercentile)

	if currentVolZRank >= 6 { // 6 = P90, 7 = P97
		if cs.bar.Metrics.NormalizedDisplacement > 0 {
			cs.bar.Metrics.AnomalyDirection = 1
		} else if cs.bar.Metrics.NormalizedDisplacement < 0 {
			cs.bar.Metrics.AnomalyDirection = -1
		} else {
			// Tie-breaker using WickAsymmetry when Open == Close
			if cs.bar.Metrics.WickAsymmetry > 0 {
				cs.bar.Metrics.AnomalyDirection = 1
			} else {
				cs.bar.Metrics.AnomalyDirection = -1
			}
		}

		// If absolute displacement is ultra-low, price is trapped despite massive volume chunks
		// 0.15 means the price moved less than 15% of the total rolling window range
		if math.Abs(cs.bar.Metrics.NormalizedDisplacement) <= 0.15 {

			if cs.bar.Metrics.WickAsymmetry > 0.10 {
				// Massive volume + No price progress + Heavy lower shadow = BUY ABSORPTION
				cs.bar.Metrics.AbsorptionSignal = 1
			} else if cs.bar.Metrics.WickAsymmetry <= -0.10 {
				// Massive volume + No price progress + Heavy upper wick = SELL ABSORPTION
				cs.bar.Metrics.AbsorptionSignal = -1
			}
		} else {
			// If displacement breaks out wide, it's a standard high-volume directional anomaly, not absorption
			if cs.bar.Metrics.NormalizedDisplacement > 0 {
				cs.bar.Metrics.AnomalyDirection = 1
			} else {
				cs.bar.Metrics.AnomalyDirection = -1
			}
		}

	}

	// ------------------------------------------------------------------------
	// VISUALIZATION LAYER SYMMETRIC COMPRESSION ENGINE
	// ------------------------------------------------------------------------

	// A1. Volume Z-Score (Absolute Historical Statistical Depth)
	if currentVolZRank > 4 && currentVolZRank > cs.bar.Metrics.PeakVolumeZRank {
		cs.bar.Metrics.PeakVolumeZRank = currentVolZRank
	} else if currentVolZRank < 4 && currentVolZRank < cs.bar.Metrics.PeakVolumeZRank {
		cs.bar.Metrics.PeakVolumeZRank = currentVolZRank
	}

	// B. Price
	currentRangeRank := getPercentileRank(tick.Enrichment.PricePercentile)
	if currentRangeRank > 4 && currentRangeRank > cs.bar.Metrics.PeakPriceRank {
		cs.bar.Metrics.PeakPriceRank = currentRangeRank
	} else if currentRangeRank < 4 && currentRangeRank < cs.bar.Metrics.PeakPriceRank {
		cs.bar.Metrics.PeakPriceRank = currentRangeRank
	}

	// C. Ticks
	currentTickRank := getPercentileRank(tick.Enrichment.TickPercentile)
	if currentTickRank > cs.bar.Metrics.PeakTickRank {
		cs.bar.Metrics.PeakTickRank = currentTickRank
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
