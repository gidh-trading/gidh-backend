package strategy

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
	Profile           *models.InstrumentProfile
}

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile
}

// NewEngine now accepts your pre-loaded profiles map keyed by stock/symbol name
func NewEngine(barLookback time.Duration, profiles map[string]*models.InstrumentProfile) *Engine {
	morningCard := NewMorningRankStrategy()
	afternoonCard := NewAfternoonReversalStrategy()

	timeBasedRouter := NewTimeBasedRouter(morningCard, afternoonCard)

	return &Engine{
		Registry:       make(map[string]*InstrumentState),
		ActiveStrategy: timeBasedRouter,
		MaxBarLookback: barLookback,
		profiles:       profiles, // Wired here
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
			Profile:           e.profiles[symbol], // Fix: Hydrate profile immediately upon lazy load
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

	symbol := enrichedTick.Raw.StockName
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
			Profile:           e.profiles[symbol], // Fix: Hydrate profile immediately upon lazy load
		}
		e.Registry[symbol] = state
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
	e.mu.Lock() // Changed to Lock to thread-safely modify CurrentSetupPhase on the state object
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	// Determine state phase
	if currentSide == "FLAT" || currentSide == "" {
		state.CurrentSetupPhase = PhaseNeutral
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
	}
	e.mu.Unlock()

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
