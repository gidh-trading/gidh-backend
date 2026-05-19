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

	// Calculate the mathematical boundary for this duration (e.g., 09:15:00 for a 5m candle hit at 09:17:34)
	candleStart := ts.Truncate(duration)

	// If the current tick's boundary is strictly newer than the candle we are building, close the old one.
	if cs.bar.Timestamp.Before(candleStart) {
		// 1. Finalize the old bar to prepare it for database insertion
		closedBar := cs.bar
		closedBar.Heatmap = cs.finalizeTransformsForUI()

		// 2. Write to the database (Assuming your DBWriter has a WriteBar/SaveBar method)
		if bm.writer != nil {
			bm.writer.AddBar(*closedBar)
		}

		// 3. Create a brand new, empty candle state starting at this new time boundary
		cs = newCandleState(candleStart, price, token, tick.Raw.StockName, timeframe)
		stateMap[token] = cs
	}

	// 4. Add the tick's data to the active candle
	bm.processTickForCandle(cs, tick, vol, timeframe)
}
