// internal/service/pipeline/bar.go

package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
)

type BarBuilderStage struct {
	loc    *time.Location
	bar1m  map[uint32]*models.Bar
	bar3m  map[uint32]*models.Bar
	bar5m  map[uint32]*models.Bar
	mu     sync.RWMutex
	writer *writer.DBWriter
	wsHub  *ws.Hub
}

func NewBarBuilderStage(w *writer.DBWriter, hub *ws.Hub) *BarBuilderStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &BarBuilderStage{
		loc:    loc,
		bar1m:  make(map[uint32]*models.Bar),
		bar3m:  make(map[uint32]*models.Bar),
		bar5m:  make(map[uint32]*models.Bar),
		writer: w,
		wsHub:  hub,
	}
}

func (s *BarBuilderStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(s.loc)
	name := tick.Raw.StockName

	// -------------------------------------------------------------------------
	// 1. INITIALIZE CURRENT BARS IF ABSENT
	// -------------------------------------------------------------------------
	if s.bar1m[token] == nil {
		s.bar1m[token] = newBar(ts, price, token, name, "1m")
	}
	if s.bar3m[token] == nil {
		s.bar3m[token] = newBar(ts, price, token, name, "3m")
	}
	if s.bar5m[token] == nil {
		s.bar5m[token] = newBar(ts, price, token, name, "5m")
	}

	// -------------------------------------------------------------------------
	// 2. UPDATE AND FLUSH 1M BARS
	// -------------------------------------------------------------------------
	b1 := s.bar1m[token]
	expected1mTs := ts.Truncate(time.Minute)

	if expected1mTs.After(b1.Timestamp) {
		if s.writer != nil {
			s.writer.AddBar(*b1)
		}
		s.bar1m[token] = newBar(ts, price, token, name, "1m")
		b1 = s.bar1m[token]
	}

	if !expected1mTs.Before(b1.Timestamp) {
		updateBar(b1, price, vol)
		b1.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b1.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b1.Ticks = append(b1.Ticks, tick.Raw)
		b1.VWAP = tick.Raw.AverageTradedPrice
		if tick.VolProfile != nil {
			b1.POC = tick.VolProfile.POC
			b1.VAH = tick.VolProfile.VAH
			b1.VAL = tick.VolProfile.VAL
		}
	}

	// -------------------------------------------------------------------------
	// 3. UPDATE AND FLUSH 3M BARS
	// -------------------------------------------------------------------------
	b3 := s.bar3m[token]
	expected3mTs := ts.Truncate(3 * time.Minute)

	if expected3mTs.After(b3.Timestamp) {
		if s.writer != nil {
			s.writer.AddBar(*b3)
		}
		s.bar3m[token] = newBar(ts, price, token, name, "3m")
		b3 = s.bar3m[token]
	}

	if !expected3mTs.Before(b3.Timestamp) {
		updateBar(b3, price, vol)
		b3.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b3.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b3.VWAP = tick.Raw.AverageTradedPrice
		if tick.VolProfile != nil {
			b3.POC = tick.VolProfile.POC
			b3.VAH = tick.VolProfile.VAH
			b3.VAL = tick.VolProfile.VAL
		}
	}

	// -------------------------------------------------------------------------
	// 4. UPDATE AND FLUSH 5M BARS
	// -------------------------------------------------------------------------
	b5 := s.bar5m[token]
	expected5mTs := ts.Truncate(5 * time.Minute)

	if expected5mTs.After(b5.Timestamp) {
		if s.writer != nil {
			s.writer.AddBar(*b5)
		}
		s.bar5m[token] = newBar(ts, price, token, name, "5m")
		b5 = s.bar5m[token]
	}

	if !expected5mTs.Before(b5.Timestamp) {
		updateBar(b5, price, vol)
		b5.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b5.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b5.VWAP = tick.Raw.AverageTradedPrice
		if tick.VolProfile != nil {
			b5.POC = tick.VolProfile.POC
			b5.VAH = tick.VolProfile.VAH
			b5.VAL = tick.VolProfile.VAL
		}
	}

	// -------------------------------------------------------------------------
	// 5. LIVE WEB_SOCKET BROADCASTS
	// -------------------------------------------------------------------------
	if s.wsHub != nil {
		s.wsHub.BroadcastJSON(name+":1m", map[string]any{"type": "bar", "data": b1})
		s.wsHub.BroadcastJSON(name+":3m", map[string]any{"type": "bar", "data": b3})
		s.wsHub.BroadcastJSON(name+":5m", map[string]any{"type": "bar", "data": b5})
	}

	return nil
}

func (s *BarBuilderStage) ClearState() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.bar1m = make(map[uint32]*models.Bar)
	s.bar3m = make(map[uint32]*models.Bar)
	s.bar5m = make(map[uint32]*models.Bar)
}
