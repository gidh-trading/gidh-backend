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
		bm.state1m[token] = newCandleState(ts, price, token, name, "1m")
	}
	if bm.state3m[token] == nil {
		bm.state3m[token] = newCandleState(ts, price, token, name, "3m")
	}
	if bm.state5m[token] == nil {
		bm.state5m[token] = newCandleState(ts, price, token, name, "5m")
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
