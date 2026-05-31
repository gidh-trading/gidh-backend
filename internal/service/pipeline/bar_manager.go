package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type BarManager struct {
	loc      *time.Location
	state1m  map[uint32]*candleState
	state3m  map[uint32]*candleState
	state5m  map[uint32]*candleState
	state10m map[uint32]*candleState
	state15m map[uint32]*candleState
	profiles map[uint32]*models.InstrumentProfile
	dnaMap   map[uint32]*models.MarketDNA
	mu       sync.RWMutex
	writer   *writer.DBWriter
	wsHub    *ws.Hub
}

func NewBarManager(w *writer.DBWriter, hub *ws.Hub, profiles map[uint32]*models.InstrumentProfile, rawDnaMap map[uint32]*models.MarketDNA) *BarManager {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return &BarManager{
		loc:      loc,
		state1m:  make(map[uint32]*candleState),
		state3m:  make(map[uint32]*candleState),
		state5m:  make(map[uint32]*candleState),
		state10m: make(map[uint32]*candleState),
		state15m: make(map[uint32]*candleState),
		profiles: profiles,
		dnaMap:   rawDnaMap,
		writer:   w,
		wsHub:    hub,
	}
}

// Process handles incoming ticks along with pre-calculated analytical snapshots
func (bm *BarManager) Process(tick *models.EnrichedTick) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(bm.loc)
	name := tick.Raw.StockName

	// Initialize states if they don't exist for the token
	if bm.state1m[token] == nil {
		bm.state1m[token] = newCandleState(ts.Truncate(time.Minute), price, token, name, "1m")
	}
	if bm.state3m[token] == nil {
		bm.state3m[token] = newCandleState(ts.Truncate(3*time.Minute), price, token, name, "3m")
	}
	if bm.state5m[token] == nil {
		bm.state5m[token] = newCandleState(ts.Truncate(5*time.Minute), price, token, name, "5m")
	}
	if bm.state10m[token] == nil {
		bm.state10m[token] = newCandleState(ts.Truncate(10*time.Minute), price, token, name, "10m")
	}
	if bm.state15m[token] == nil {
		bm.state15m[token] = newCandleState(ts.Truncate(15*time.Minute), price, token, name, "15m")
	}

	// Route tracking to individual timeframes
	bm.updateTimeframe(bm.state1m, token, ts, price, vol, time.Minute, "1m", tick)
	bm.updateTimeframe(bm.state3m, token, ts, price, vol, 3*time.Minute, "3m", tick)
	bm.updateTimeframe(bm.state5m, token, ts, price, vol, 5*time.Minute, "5m", tick)
	bm.updateTimeframe(bm.state10m, token, ts, price, vol, 10*time.Minute, "10m", tick)
	bm.updateTimeframe(bm.state15m, token, ts, price, vol, 15*time.Minute, "15m", tick)

	// Tick-by-Tick Continuous Broadcasting to WebSockets
	if bm.wsHub != nil {
		timeframes := []map[uint32]*candleState{bm.state1m, bm.state3m, bm.state5m, bm.state10m, bm.state15m}
		for _, stateMap := range timeframes {
			cs := stateMap[token]
			if cs != nil && cs.bar != nil {
				bm.wsHub.BroadcastJSON(cs.bar.StockName+":"+cs.bar.Timeframe, map[string]any{
					"type": "bar",
					"data": cs.bar,
				})
			}
		}
	}

	return nil
}

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
		closedBar := cs.bar

		if bm.wsHub != nil {
			bm.wsHub.BroadcastJSON(tick.Raw.StockName+":"+timeframe, map[string]any{
				"type": "bar",
				"data": closedBar,
			})
		}

		if bm.writer != nil {
			bm.writer.AddBar(*closedBar)
		}

		cs.bar = newBar(candleStart, price, token, tick.Raw.StockName, timeframe)
	}

	bm.processTickForCandle(cs, tick, vol, timeframe)
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

func (bm *BarManager) GetActiveBarsSnapshot(token uint32) map[string]*models.Bar {
	bm.mu.RLock()
	defer bm.mu.RUnlock()

	snapshot := make(map[string]*models.Bar)
	if cs, ok := bm.state1m[token]; ok && cs != nil {
		snapshot["1m"] = cs.bar
	}
	if cs, ok := bm.state3m[token]; ok && cs != nil {
		snapshot["3m"] = cs.bar
	}
	if cs, ok := bm.state5m[token]; ok && cs != nil {
		snapshot["5m"] = cs.bar
	}
	if cs, ok := bm.state10m[token]; ok && cs != nil {
		snapshot["10m"] = cs.bar
	}
	if cs, ok := bm.state15m[token]; ok && cs != nil {
		snapshot["15m"] = cs.bar
	}

	return snapshot
}
