// internal/service/pipeline/bar_manager.go

package pipeline

import (
	"context"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type BarManager struct {
	loc           *time.Location
	state1m       map[uint32]*candleState
	state3m       map[uint32]*candleState
	state5m       map[uint32]*candleState
	lastTickState map[uint32]*tokenTickState // Cache to evaluate continuous delta rules

	mu     sync.RWMutex
	writer *writer.DBWriter
	wsHub  *ws.Hub
}

func NewBarManager(w *writer.DBWriter, hub *ws.Hub) *BarManager {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return &BarManager{
		loc:           loc,
		state1m:       make(map[uint32]*candleState),
		state3m:       make(map[uint32]*candleState),
		state5m:       make(map[uint32]*candleState),
		lastTickState: make(map[uint32]*tokenTickState),
		writer:        w,
		wsHub:         hub,
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

	// 1. Lazy Initialization of active frame states
	if bm.state1m[token] == nil {
		bm.state1m[token] = newCandleState(ts.Truncate(time.Minute), price, token, name, "1m")
	}
	if bm.state3m[token] == nil {
		bm.state3m[token] = newCandleState(ts.Truncate(3*time.Minute), price, token, name, "3m")
	}
	if bm.state5m[token] == nil {
		bm.state5m[token] = newCandleState(ts.Truncate(5*time.Minute), price, token, name, "5m")
	}
	if bm.lastTickState[token] == nil {
		bm.lastTickState[token] = &tokenTickState{lastPrice: price}
	}

	// 2. Cascade calculations down interval streams
	bm.updateTimeframe(bm.state1m, token, ts, price, vol, time.Minute, "1m", tick)
	bm.updateTimeframe(bm.state3m, token, ts, price, vol, 3*time.Minute, "3m", tick)
	bm.updateTimeframe(bm.state5m, token, ts, price, vol, 5*time.Minute, "5m", tick)

	return nil
}

// updateTimeframe checks if a new candle boundary has been crossed, flushes the old one, and processes the tick.
func (bm *BarManager) updateTimeframe(
	stateMap map[uint32]*candleState,
	token uint32,
	ts time.Time,
	price float64,
	vol float64,
	duration time.Duration,
	timeframe string,
	tick *models.EnrichedTick,
) {
	cs := stateMap[token]
	candleStart := ts.Truncate(duration)

	if cs.bar.Timestamp.Before(candleStart) {
		// 1. Finalize the old bar to prepare it for database insertion
		closedBar := cs.bar
		closedBar.Heatmap = cs.finalizeTransformsForUI()
		closedBar.Slopes = cs.finalizeSlopesForUI()

		if bm.wsHub != nil {
			bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{
				"type": "bar",
				"data": closedBar,
			})
		}

		if bm.writer != nil {
			bm.writer.AddBar(*closedBar)
		}

		// ----------------------------------------------------
		// 📈 UPDATE MACRO REGRESSION (NEW)
		// ----------------------------------------------------
		// Normalize X-Axis to the minute of the day for stable math
		x := float64(closedBar.Timestamp.Hour()*60 + closedBar.Timestamp.Minute())

		// Add this closed bar to the running regression totals
		cs.macroQueue = append(cs.macroQueue, macroPoint{
			x: x, price: closedBar.Close, vwap: closedBar.VWAP, volume: closedBar.Volume,
		})
		cs.PriceReg.Add(x, closedBar.Close)
		cs.VWAPReg.Add(x, closedBar.VWAP)
		cs.VolReg.Add(x, closedBar.Volume)

		// O(1) Eviction: Keep the macro window strictly at the last 10 bars
		if len(cs.macroQueue) > 10 {
			old := cs.macroQueue[0]
			cs.macroQueue = cs.macroQueue[1:] // Pop the oldest bar

			cs.PriceReg.Remove(old.x, old.price)
			cs.VWAPReg.Remove(old.x, old.vwap)
			cs.VolReg.Remove(old.x, old.volume)
		}

		// 3. Reset ONLY the bar, heatmaps, and max values for the new candle
		// DO NOT overwrite `cs`, otherwise you delete the regressions!
		cs.bar = newBar(candleStart, price, token, tick.Raw.StockName, timeframe)
		cs.heatmapMap = make(map[float64]*models.HeatmapCell)

		cs.mpMap = make(map[int]float64)
		cs.mvMap = make(map[int]float64)
		cs.mvolMap = make(map[int]float64)

		cs.maxMp = 0
		cs.maxMv = 0
		cs.maxMvol = 0

	}

	// 4. Add the tick's data to the active candle
	bm.processTickForCandle(cs, tick, vol, timeframe)
}

// StartBroadcastingEngine spins up a fixed-interval background worker that samples
// tracking state maps and broadcasts stable layout frames completely decoupled from raw tick bursts.
func (bm *BarManager) StartBroadcastingEngine(ctx context.Context, broadcastRate time.Duration) {
	ticker := time.NewTicker(broadcastRate)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bm.mu.Lock()
			if bm.wsHub == nil {
				bm.mu.Unlock()
				continue
			}

			// Slice up all tracked interval categories so they all get smoothly sampled
			timeframeMaps := []map[uint32]*candleState{bm.state1m, bm.state3m, bm.state5m}

			for _, stateMap := range timeframeMaps {
				for _, cs := range stateMap {
					if cs == nil || cs.bar == nil {
						continue
					}

					// Safely capture stable transforms without pipeline interruptions
					cs.bar.Heatmap = cs.finalizeTransformsForUI()
					cs.bar.Slopes = cs.finalizeSlopesForUI()

					// Broadcast a single uniform layout down the specific WS channels
					bm.wsHub.BroadcastJSON(cs.bar.StockName+":"+cs.bar.Timeframe, map[string]any{
						"type": "bar",
						"data": cs.bar,
					})
				}
			}
			bm.mu.Unlock()

		case <-ctx.Done():
			return
		}
	}
}
