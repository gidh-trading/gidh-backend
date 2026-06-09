package strategy

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
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
	LatestDirection  models.DirectionState // e.g., "BULLISH_ABSORPTION"
	LatestVolumeRank int                   // Peak-locked rank (0, 90, 95, 97, 99)
	LatestPriceRank  int                   // Candle body size percentile
	LiveSessionVWAP  float64               // Anchor line from exchange

	// --- 📈 Candlestick Structural Properties ---
	LatestBodySize  float64 // Absolute size of the candle body (|Close - Open|)
	LatestLowerWick float64 // Absolute size of the lower wick
	LatestWickRatio float64 // Lower wick size divided by total high-to-low range

	// --- ⏱️ Strategy Time Constraints ---
	MinutesSinceOpen int // Minutes elapsed since market open (9:15 AM IST)

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase // e.g., "NEUTRAL" or "ACTIVE_TRADE"

	// --- Volatility Space ---
	NormalizedVwapDistance float64 // Distance from VWAP scaled by ADRPct

	// --- Memory Storage ---
	BarHistory map[string][]*models.Bar  // Holds historical closed bars
	Profile    *models.InstrumentProfile // Stores ADRPct and ADV constants
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
	// Dummy initializers assuming these exist in your repository packages
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

// IngestClosedBar caches historical timeframes and computes metrics upon bar close
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	if bar == nil {
		return
	}

	// 🛑 TIME GATE: Ignore anything before the official 9:15 AM market open
	currentTimeHM := (bar.Timestamp.Hour() * 100) + bar.Timestamp.Minute()
	if currentTimeHM < 915 {
		return // Silently drop pre-market auction bars
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
			Profile:           e.profiles[symbol], // Hydrate profile immediately upon lazy load
		}
		e.Registry[symbol] = state
	}

	// 1. Process Core Base Metric Assignments
	state.LastUpdated = bar.Timestamp
	state.LatestPrice = bar.Close
	state.LatestDirection = bar.Analytics.Direction
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank
	state.LiveSessionVWAP = bar.VWAP

	// 2. Compute Candle Geometric Proportions
	totalRange := bar.High - bar.Low
	state.LatestBodySize = math.Abs(bar.Close - bar.Open)

	if totalRange > 0 {
		candleBodyBottom := math.Min(bar.Open, bar.Close)
		state.LatestLowerWick = candleBodyBottom - bar.Low
		state.LatestWickRatio = state.LatestLowerWick / totalRange
	} else {
		state.LatestLowerWick = 0.0
		state.LatestWickRatio = 0.0
	}

	// 3. Compute Minutes Since Open (09:15 AM IST)
	// bar.Timestamp comes localized from BarManager in Asia/Kolkata
	state.MinutesSinceOpen = (bar.Timestamp.Hour() * 60) + bar.Timestamp.Minute() - 555

	// 4. Calculate Normalized Spatial Distance from VWAP
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	} else {
		state.NormalizedVwapDistance = 0.0
	}

	// 5. Append and prune historical frames
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

// UpdateContext updates real-time tracking metrics between bar closes
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
			Profile:           e.profiles[symbol], // Hydrate profile immediately upon lazy load
		}
		e.Registry[symbol] = state
	}

	rawTick := enrichedTick.Raw
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LiveSessionVWAP = rawTick.AverageTradedPrice

	// Live context calculations between bar closes
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	}
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
