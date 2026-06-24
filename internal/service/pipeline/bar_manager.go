package pipeline

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
)

type InstrumentBarState struct {
	mu       sync.Mutex
	state1m  *candleState
	state3m  *candleState
	state5m  *candleState
	state10m *candleState
	state15m *candleState
}

type candleState struct {
	bar     *models.Bar
	history *TimeframeAnalyticsHistory // Isolated stateful engine container tracking safely per asset token frame
}

type BarManager struct {
	loc             *time.Location
	instruments     map[uint32]*InstrumentBarState
	profiles        map[uint32]*models.InstrumentProfile
	dnaMap          map[uint32]*models.MarketDNA
	mu              sync.RWMutex
	wsHub           *ws.Hub
	dbWriter        *writer.DBWriter
	analyticsEngine *BarAnalyticsEngine
	MacroListener   interface{ IngestClosedBar(bar *models.Bar) }
}

func NewBarManager(
	hub *ws.Hub,
	dbW *writer.DBWriter,
	profiles map[uint32]*models.InstrumentProfile,
	rawDnaMap map[uint32]*models.MarketDNA,
) *BarManager {
	loc, _ := time.LoadLocation("Asia/Kolkata")
	return &BarManager{
		loc:             loc,
		instruments:     make(map[uint32]*InstrumentBarState),
		profiles:        profiles,
		dnaMap:          rawDnaMap,
		wsHub:           hub,
		dbWriter:        dbW,
		analyticsEngine: NewBarAnalyticsEngine(rawDnaMap, profiles, dbW),
	}
}

func (bm *BarManager) Process(tick *models.EnrichedTick) error {
	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(bm.loc)
	name := tick.Raw.StockName

	bm.mu.RLock()
	ibs, exists := bm.instruments[token]
	bm.mu.RUnlock()

	if !exists {
		bm.mu.Lock()
		ibs, exists = bm.instruments[token]
		if !exists {
			ibs = &InstrumentBarState{
				state1m:  newCandleState(ts.Truncate(time.Minute), price, token, name, "1m"),
				state3m:  newCandleState(ts.Truncate(3*time.Minute), price, token, name, "3m"),
				state5m:  newCandleState(ts.Truncate(5*time.Minute), price, token, name, "5m"),
				state10m: newCandleState(ts.Truncate(10*time.Minute), price, token, name, "10m"),
				state15m: newCandleState(ts.Truncate(15*time.Minute), price, token, name, "15m"),
			}
			bm.instruments[token] = ibs
		}
		bm.mu.Unlock()
	}

	ibs.mu.Lock()
	defer ibs.mu.Unlock()

	bm.updateTimeframe(ibs, token, ts, price, vol, time.Minute, "1m", tick)
	bm.updateTimeframe(ibs, token, ts, price, vol, 3*time.Minute, "3m", tick)
	bm.updateTimeframe(ibs, token, ts, price, vol, 5*time.Minute, "5m", tick)
	bm.updateTimeframe(ibs, token, ts, price, vol, 10*time.Minute, "10m", tick)
	bm.updateTimeframe(ibs, token, ts, price, vol, 15*time.Minute, "15m", tick)

	if bm.wsHub != nil {
		states := []*candleState{ibs.state1m, ibs.state3m, ibs.state5m, ibs.state10m, ibs.state15m}
		for _, cs := range states {
			if cs != nil && cs.bar != nil {
				bm.analyticsEngine.PopulateLiveAnalytics(cs.bar, cs.history)
				bm.wsHub.BroadcastJSON(cs.bar.StockName+":"+cs.bar.Timeframe, map[string]any{
					"type": "bar",
					"data": cs.bar,
				})
			}
		}
	}

	return nil
}

func (bm *BarManager) GetActiveBarsSnapshot(token uint32) map[string]*models.Bar {
	bm.mu.RLock()
	ibs, ok := bm.instruments[token]
	bm.mu.RUnlock()

	snapshot := make(map[string]*models.Bar)
	if !ok || ibs == nil {
		return snapshot
	}

	ibs.mu.Lock()
	defer ibs.mu.Unlock()

	if ibs.state1m != nil {
		snapshot["1m"] = ibs.state1m.bar
	}
	if ibs.state3m != nil {
		snapshot["3m"] = ibs.state3m.bar
	}
	if ibs.state5m != nil {
		snapshot["5m"] = ibs.state5m.bar
	}
	if ibs.state10m != nil {
		snapshot["10m"] = ibs.state10m.bar
	}
	if ibs.state15m != nil {
		snapshot["15m"] = ibs.state15m.bar
	}

	return snapshot
}

func (bm *BarManager) ClearState() {
	bm.mu.Lock()
	bm.instruments = make(map[uint32]*InstrumentBarState)
	bm.mu.Unlock()

	logger.Info("Bar Manager historical window cache states wiped cleanly.")
}

func newCandleState(ts time.Time, price float64, token uint32, name, timeframe string) *candleState {
	return &candleState{
		bar: newBar(ts, price, token, name, timeframe),
		history: &TimeframeAnalyticsHistory{
			TotalBars: 0,
		},
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
		Analytics: models.BarAnalytics{
			VolumeRank: 0,
			TickRank:   0,
			PriceRank:  0,
			RangeRank:  0,
			Direction:  models.DirSideways,
		},
	}
}

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
	cs.bar.TickCount++

	cs.bar.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
	cs.bar.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
	cs.bar.VWAP = tick.Raw.AverageTradedPrice

	prevClose := tick.Raw.LastPrice - tick.Raw.Change
	if prevClose > 0 {
		cs.bar.ChangePct = (tick.Raw.Change / prevClose) * 100
	}

	if tick.VolProfile != nil {
		cs.bar.POC = tick.VolProfile.POC
		cs.bar.VAH = tick.VolProfile.VAH
		cs.bar.VAL = tick.VolProfile.VAL
	}

	if timeframe == "1m" {
		cs.bar.Ticks = append(cs.bar.Ticks, tick.Raw)
	}
}

func (bm *BarManager) updateTimeframe(
	ibs *InstrumentBarState,
	token uint32,
	ts time.Time,
	price float64,
	vol float64,
	duration time.Duration,
	timeframe string,
	tick *models.EnrichedTick,
) {
	var cs *candleState
	switch timeframe {
	case "1m":
		cs = ibs.state1m
	case "3m":
		cs = ibs.state3m
	case "5m":
		cs = ibs.state5m
	case "10m":
		cs = ibs.state10m
	case "15m":
		cs = ibs.state15m
	}

	candleStart := ts.Truncate(duration)

	if cs.bar.Timestamp.Before(candleStart) {
		closedBar := cs.bar

		if bm.analyticsEngine != nil {
			bm.analyticsEngine.AnalyzeClose(closedBar, cs.history)
		}

		if bm.MacroListener != nil {
			bm.MacroListener.IngestClosedBar(closedBar)
		}

		cs.bar = newBar(candleStart, price, token, tick.Raw.StockName, timeframe)
	}

	bm.processTickForCandle(cs, tick, vol, timeframe)

	if bm.analyticsEngine != nil {
		bm.analyticsEngine.AnalyzeTick(cs.bar, tick)
	}
}
