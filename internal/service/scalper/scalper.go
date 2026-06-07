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

	// 2. Put your cards into the time-based router traffic cop
	timeBasedRouter := NewTimeBasedRouter(morningCard, morningCard)

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
func (e *Engine) UpdateContext(symbol string, price float64, timestamp time.Time, direction models.DirectionState, volRank int, priceRank int) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
		}
		e.Registry[symbol] = state
	}

	state.LatestPrice = price
	state.LastUpdated = timestamp
	state.LatestDirection = direction
	state.LatestVolumeRank = volRank
	state.LatestPriceRank = priceRank
}

// GenerateSignal checks inventory states and routes data down to your router rules
func (e *Engine) GenerateSignal(symbol string, currentSide string) string {
	e.mu.RLock()
	state, exists := e.Registry[symbol]
	e.mu.RUnlock()

	if !exists || e.ActiveStrategy == nil {
		return "HOLD"
	}

	if currentSide == "FLAT" || currentSide == "" {
		state.CurrentSetupPhase = PhaseNeutral
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
	}

	if state.CurrentSetupPhase == PhaseNeutral {
		return e.ActiveStrategy.CheckEntry(state)
	}

	if state.CurrentSetupPhase == PhaseActiveTrade {
		return e.ActiveStrategy.CheckExit(state, currentSide)
	}

	return "HOLD"
}
