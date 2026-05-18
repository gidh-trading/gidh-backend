package pipeline

import (
	"gidh-backend/internal/service/models"
	"math"
	"time"
)

// tokenTickState stores top-of-book historical properties from the immediate previous tick.
type tokenTickState struct {
	lastPrice    float64
	lastBidPrice float64
	lastBidQty   float64
	lastAskPrice float64
	lastAskQty   float64
}

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

// finalizeTransforms flushes map structures into the passive database carrier slice array.
func (cs *candleState) finalizeTransforms() *models.Bar {
	cells := make([]models.HeatmapCell, 0, len(cs.heatmapMap))
	for _, cell := range cs.heatmapMap {
		cells = append(cells, *cell)
	}
	cs.bar.Heatmap = cells
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

func (bm *BarManager) accumulateMicrostructure(cs *candleState, tick *models.EnrichedTick, tickVol float64) {
	// Extract structures
	depth := tick.Raw.Depth
	lastState := bm.lastTickState[tick.Raw.InstrumentToken]

	// Guard against empty books safely
	if len(depth.Buy) == 0 || len(depth.Sell) == 0 {
		return
	}

	// Capture Top of Book parameters
	currBidP := depth.Buy[0].Price
	currBidQ := float64(depth.Buy[0].Quantity)
	currAskP := depth.Sell[0].Price
	currAskQ := float64(depth.Sell[0].Quantity)

	// -------------------------------------------------------------------------
	// A. TRADE CLASSIFICATION LOGIC (Lee-Ready / Aggressive Execution Flow)
	// -------------------------------------------------------------------------
	var aggBuy, aggSell float64
	if tickVol > 0 {
		tradePrice := tick.Raw.LastPrice
		switch {
		case tradePrice >= currAskP:
			aggBuy = tickVol
		case tradePrice <= currBidP:
			aggSell = tickVol
		default:
			// Tick Rule Fallback
			if tradePrice > lastState.lastPrice {
				aggBuy = tickVol
			} else if tradePrice < lastState.lastPrice {
				aggSell = tickVol
			}
		}
	}

	// -------------------------------------------------------------------------
	// B. RESTING DEPTH METRICS (Step-Weighted Imbalance)
	// -------------------------------------------------------------------------
	var weightedBid, weightedAsk float64
	for i := 0; i < len(depth.Buy); i++ {
		weight := 1.0 / float64(i+1) // Closer levels carry higher importance scaling weights
		weightedBid += float64(depth.Buy[i].Quantity) * weight
	}
	for i := 0; i < len(depth.Sell); i++ {
		weight := 1.0 / float64(i+1)
		weightedAsk += float64(depth.Sell[i].Quantity) * weight
	}

	var weightedImbalance float64
	if (weightedBid + weightedAsk) > 0 {
		weightedImbalance = (weightedBid - weightedAsk) / (weightedBid + weightedAsk)
	}

	// -------------------------------------------------------------------------
	// C. ORDER BOOK FLOW DYNAMICS (Consumption, Replenishment, Fair Value)
	// -------------------------------------------------------------------------
	var consumedBid, consumedAsk, replenishmentBid, replenishmentAsk, normalizedVOFI float64

	// Microprice: Imbalance-adjusted short-term true fair value estimation
	microPrice := (currAskP*currBidQ + currBidP*currAskQ) / (currBidQ + currAskQ)

	if lastState.lastBidPrice > 0 && lastState.lastAskPrice > 0 {
		// 1. Liquidity Consumption (Cancellations or execution absorption floors)
		if currAskP == lastState.lastAskPrice && currAskQ < lastState.lastAskQty {
			consumedAsk = lastState.lastAskQty - currAskQ
		}
		if currBidP == lastState.lastBidPrice && currBidQ < lastState.lastBidQty {
			consumedBid = lastState.lastBidQty - currBidQ
		}

		// 2. Iceberg Replenishment Verification
		if currAskP == lastState.lastAskPrice && aggBuy > 0 {
			actualDrop := lastState.lastAskQty - currAskQ
			if aggBuy > actualDrop {
				replenishmentAsk = aggBuy - actualDrop // Restocking detected on the Offer
			}
		}
		if currBidP == lastState.lastBidPrice && aggSell > 0 {
			actualDrop := lastState.lastBidQty - currBidQ
			if aggSell > actualDrop {
				replenishmentBid = aggSell - actualDrop // Restocking detected on the Bid
			}
		}

		// 3. Normalized Volume Order Flow Imbalance (VOFI Formulation)
		var deltaBid, deltaAsk float64
		if currBidP > lastState.lastBidPrice {
			deltaBid = currBidQ
		} else if currBidP == lastState.lastBidPrice {
			deltaBid = currBidQ - lastState.lastBidQty
		} else {
			deltaBid = -lastState.lastBidQty
		}

		if currAskP < lastState.lastAskPrice {
			deltaAsk = currAskQ
		} else if currAskP == lastState.lastAskPrice {
			deltaAsk = currAskQ - lastState.lastAskQty
		} else {
			deltaAsk = -lastState.lastAskQty
		}

		// Divide by traded volume to prevent noise/illiquid metric explosions
		normalizedVOFI = (deltaBid - deltaAsk) / math.Max(tickVol, 1.0)
	}

	// -------------------------------------------------------------------------
	// D. O(1) MAP-BACKED HEATMAP MATRIX UPDATES
	// -------------------------------------------------------------------------
	bin := tick.AnomalyBin
	if !tick.HasAnomaly {
		// Fallback anchor configuration if stage evaluation rule didn't explicitly flag this tick
		bin = math.Floor(tick.Raw.LastPrice/cs.bar.Open) * cs.bar.Open
		if bin == 0 {
			bin = tick.Raw.LastPrice
		}
	}

	cell, exists := cs.heatmapMap[bin]
	if !exists {
		cell = &models.HeatmapCell{PriceBin: bin}
		cs.heatmapMap[bin] = cell
	}

	// Increment metrics incrementally
	if tick.HasAnomaly {
		cell.AnomalyCount++
		if cell.AnomalyCount > cs.maxAnomalyCount {
			cs.maxAnomalyCount = cell.AnomalyCount // O(1) Incremental baseline assignment peak tracking
		}
	}

	// Accumulate metrics continuously
	cell.CellVolume += tickVol
	cell.AggressiveBuyVol += aggBuy
	cell.AggressiveSellVol += aggSell
	cell.DepthImbalance = weightedImbalance
	cell.OrderFlowDelta = normalizedVOFI
	cell.ConsumedBidLiq += consumedBid
	cell.ConsumedAskLiq += consumedAsk
	cell.ReplenishmentBid += replenishmentBid
	cell.ReplenishmentAsk += replenishmentAsk
	cell.MicroPrice = microPrice

	// Rescale intensities dynamically
	if cs.maxAnomalyCount > 0 {
		cell.IntensityScore = float64(cell.AnomalyCount) / float64(cs.maxAnomalyCount)
	}
	if cs.bar.Volume > 0 {
		cell.VolumeRatio = cell.CellVolume / cs.bar.Volume
	}

	// 4. Update the continuous baseline cache reference context
	lastState.lastPrice = tick.Raw.LastPrice
	lastState.lastBidPrice = currBidP
	lastState.lastBidQty = currBidQ
	lastState.lastAskPrice = currAskP
	lastState.lastAskQty = currAskQ
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.state1m = make(map[uint32]*candleState)
	bm.state3m = make(map[uint32]*candleState)
	bm.state5m = make(map[uint32]*candleState)
	bm.lastTickState = make(map[uint32]*tokenTickState)
}
