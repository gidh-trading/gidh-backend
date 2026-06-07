package scalper

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

type InstrumentState struct {
	Symbol           string
	LastUpdated      time.Time
	LatestPrice      float64
	LatestDirection  models.DirectionState
	LatestVolumeRank int
	LatestPriceRank  int
	LiveSessionVWAP  float64

	BarHistory        map[string][]*models.Bar
	CurrentSetupPhase SetupPhase
}

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
}

// NewEngine initializes the core decoupled infrastructure container
func NewEngine(barLookback time.Duration) *Engine {
	// 1. FIXED: Calling the correct strategy constructor
	morningCard := NewMorningRankStrategy()
	afternoonCard := NewAfternoonReversalStrategy()

	// 2. Put your cards into the time-based router traffic cop
	timeBasedRouter := NewTimeBasedRouter(morningCard, afternoonCard)

	// 3. Construct and return the infrastructure engine shell
	return &Engine{
		Registry:       make(map[string]*InstrumentState),
		ActiveStrategy: timeBasedRouter,
		MaxBarLookback: barLookback,
	}
}

// IngestClosedBar caches historical timeframes and prunes old data elements
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	if bar == nil {
		return
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	symbol := bar.StockName
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
		}
		e.Registry[symbol] = state
	}

	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	barCutoff := bar.Timestamp.Add(-e.MaxBarLookback)
	validIdx := 0
	for i, historicalBar := range state.BarHistory[tf] {
		if historicalBar.Timestamp.Before(barCutoff) {
			validIdx = i + 1
		} else {
			break
		}
	}
	if validIdx > 0 {
		state.BarHistory[tf] = state.BarHistory[tf][validIdx:]
	}
}

// UpdateContext accepts streaming micro-ticks
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, exists := e.Registry[enrichedTick.Raw.StockName]
	if !exists {
		state = &InstrumentState{
			Symbol:            enrichedTick.Raw.StockName,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
		}
		e.Registry[enrichedTick.Raw.StockName] = state
	}

	rawTick := enrichedTick.Raw
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LatestDirection = enrichedTick.Enrichment.Direction
	state.LatestVolumeRank = enrichedTick.Enrichment.VolumeRank
	state.LatestPriceRank = enrichedTick.Enrichment.PriceRank
	state.LiveSessionVWAP = rawTick.AverageTradedPrice
}

// GenerateSignal checks inventory states and routes data down to your router rules
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.RLock()
	state, exists := e.Registry[symbol]
	e.mu.RUnlock()

	if !exists || e.ActiveStrategy == nil {
		return "HOLD"
	}

	// Determine state phase
	if currentSide == "FLAT" || currentSide == "" {
		state.CurrentSetupPhase = PhaseNeutral
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
	}

	// Track Track A: Flat position lookups
	if state.CurrentSetupPhase == PhaseNeutral {
		return e.ActiveStrategy.CheckEntry(state)
	}

	// Track Track B: Active Trade sequential checklist evaluation
	if state.CurrentSetupPhase == PhaseActiveTrade {

		// 🛑 CHECK 1: Evaluate safety Stop Loss first!
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			return "EXIT_" + currentSide
		}

		// 🎯 CHECK 2: Evaluate hard cash Take Profit target next!
		if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
			return "EXIT_" + currentSide
		}

		// 📉 CHECK 3: If targets are clear, look at indicator trend exits
		return e.ActiveStrategy.CheckExit(state, currentSide)
	}

	return "HOLD"
}
