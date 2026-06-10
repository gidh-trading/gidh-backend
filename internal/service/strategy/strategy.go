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

// InstrumentState tracks stable, macro-structural session context instead of frantic speed.
type InstrumentState struct {
	Symbol          string
	LastUpdated     time.Time
	LatestPrice     float64
	LiveSessionVWAP float64 // The ultimate anchor line from the exchange

	// --- 🗺️ Daily Opening Landscape Context ---
	IsGapUp            bool    // Locked via first tick change percent
	IsGapDown          bool    // Locked via first tick change percent
	InitialOpenPrice   float64 // Captured from the first 1-minute bar of the session
	EntryVwapAnchor    float64 // Captures and freezes the exact VWAP price at the moment of entry
	HasInitializedGaps bool    // Tracker flag to freeze opening context

	// --- 📊 VWAP Live Acceptance Tracking ---
	ConsecutiveClosesAboveVwap int  // Rolling block tracker of sustained presence over anchor
	ConsecutiveClosesBelowVwap int  // Rolling block tracker of sustained presence under anchor
	IsVwapAcceptanceConfirmed  bool // Flips true when trend dominance is mathematically confirmed

	// --- 🥊 The Institutional Ledger (Tug of War) ---
	// Accumulates absolute volume traded on high-conviction, directional bars
	BullishPushVolume float64 // Absolute shares committed to attacking the offer
	BearishPushVolume float64 // Absolute shares committed to slamming the bid

	// --- 🔄 Real-Time Spatial Snapshots ---
	LatestVolumeRank       int     // Captured from incoming closed bar metrics
	LatestPriceRank        int     // Percentage representation of body size
	NormalizedVwapDistance float64 // Distance from VWAP scaled by ADRPct
	PeakVwapExtension      float64 // Maximum absolute distance reached during active trade tracking

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase
	LastTradedBarTime time.Time
	BarHistory        map[string][]*models.Bar  // Holds historical closed bars
	Profile           *models.InstrumentProfile // Stores ADRPct and ADV constants
}

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile

	// --- 📊 Optimization Logger Integrations ---
	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog) // Hook for database saving / backtest logs
}

// NewEngine accepts pre-loaded profiles map and an active trade logging callback hook.
func NewEngine(barLookback time.Duration, profiles map[string]*models.InstrumentProfile, completeHook func(log *OptimizationTradeLog)) *Engine {
	ledgerStrategyCard := NewInstitutionalLedgerStrategy()

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   ledgerStrategyCard,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		OnTradeCompleted: completeHook,
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
			Profile:           e.profiles[symbol],
		}
		e.Registry[symbol] = state
	}

	// 1. Process Core Base Metric Assignments
	state.LastUpdated = bar.Timestamp
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank

	// 2. Capture baseline bar open for layout logging reference
	if state.InitialOpenPrice == 0 {
		state.InitialOpenPrice = bar.Open
	}

	// 3. Track Time-at-Price VWAP Acceptance Counters (3 Continuous Closes)
	if bar.Close > bar.VWAP {
		state.ConsecutiveClosesAboveVwap++
		state.ConsecutiveClosesBelowVwap = 0
		if state.ConsecutiveClosesAboveVwap >= 3 {
			state.IsVwapAcceptanceConfirmed = true
		}
	} else if bar.Close < bar.VWAP {
		state.ConsecutiveClosesBelowVwap++
		state.ConsecutiveClosesAboveVwap = 0
		if state.ConsecutiveClosesBelowVwap >= 3 {
			state.IsVwapAcceptanceConfirmed = true
		}
	}

	// 4. Update the Institutional Volume Effectiveness Ledger Balance Sheet
	analytics := bar.Analytics
	if state.LatestVolumeRank >= 6 && state.LatestPriceRank >= 6 {
		switch analytics.Direction {
		case models.DirStrongBullish, models.DirBullish:
			state.BullishPushVolume += bar.Volume
		case models.DirStrongBearish, models.DirBearish:
			state.BearishPushVolume += bar.Volume
		}
	}

	// 5. Calculate Normalized Spatial Distance from VWAP
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	} else {
		state.NormalizedVwapDistance = 0.0
	}

	// 6. Append and prune historical frames
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

// UpdateContext updates real-time tracking metrics and evaluates active trailing locks
// and dynamic tick-level entries/exits live on every incoming tick.
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:            symbol,
			BarHistory:        make(map[string][]*models.Bar),
			CurrentSetupPhase: PhaseNeutral,
			Profile:           e.profiles[symbol],
		}
		e.Registry[symbol] = state
	}

	rawTick := enrichedTick.Raw
	state.LatestPrice = rawTick.LastPrice
	state.LastUpdated = rawTick.Timestamp
	state.LiveSessionVWAP = rawTick.AverageTradedPrice

	// 1. Initialize gap structure using the Change Percent vector on the very first session tick
	if !state.HasInitializedGaps {
		if rawTick.Change > 0.0 {
			state.IsGapUp = true
			state.IsGapDown = false
		} else if rawTick.Change < 0.0 {
			state.IsGapDown = true
			state.IsGapUp = false
		}
		state.HasInitializedGaps = true
	}

	// 2. Calculate Normalized Spatial Distance from VWAP scaled by ADRPct
	if state.LiveSessionVWAP > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
		rawDistancePct := ((state.LatestPrice - state.LiveSessionVWAP) / state.LiveSessionVWAP) * 100
		state.NormalizedVwapDistance = rawDistancePct / state.Profile.ADRPct
	} else {
		state.NormalizedVwapDistance = 0.0
	}

	absAdrDistance := math.Abs(state.NormalizedVwapDistance)

	// Hardcoded strategy parameter mapping alignment for the engine's tick scanner
	// (Ensure this perfectly matches your s.AdrScaleMultiplier configuration, e.g., 0.05 = 5% of ADR)
	adrMultiplier := 0.05

	// =========================================================================
	// 🎯 TRACK A: FLAT ENTRY TICK EVALUATION
	// =========================================================================
	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
		state.PeakVwapExtension = 0.0

		// Check structural setup alignment from the ledger strategy card
		setupSignal := e.ActiveStrategy.CheckEntry(state)

		// Setup Variant A: Trigger tick-level pullback zone execution the second it hits the cushion
		if setupSignal == "SETUP_READY_LONG" && absAdrDistance <= adrMultiplier {
			e.mu.Unlock()
			return "GO_LONG"
		}
		if setupSignal == "SETUP_READY_SHORT" && absAdrDistance <= adrMultiplier {
			e.mu.Unlock()
			return "GO_SHORT"
		}

		// Setup Variant B: Instantly pass through direct aggressive runaway momentum breakout triggers
		if setupSignal == "GO_LONG" || setupSignal == "GO_SHORT" {
			e.mu.Unlock()
			return setupSignal
		}
	}

	// =========================================================================
	// 🎯 TRACK B: ACTIVE POSITION TICK EVALUATION
	// =========================================================================
	if !isFlatNow {
		state.CurrentSetupPhase = PhaseActiveTrade

		// 🔒 UN-WARPED TRACKING: Calculate distance relative to your frozen entry baseline anchor
		var currentExtension float64
		if state.EntryVwapAnchor > 0 && state.Profile != nil && state.Profile.ADRPct > 0 {
			// Calculate percentage distance between current tick price and frozen entry VWAP
			rawAnchorDistancePct := ((state.LatestPrice - state.EntryVwapAnchor) / state.EntryVwapAnchor) * 100
			// Scale by ADR to match the rest of your volatility architecture
			currentExtension = math.Abs(rawAnchorDistancePct / state.Profile.ADRPct)
		} else {
			// Fallback to live metric if anchor wasn't cached safely
			currentExtension = math.Abs(state.NormalizedVwapDistance)
		}

		// Update the peak locked distance record using this stable benchmark
		if currentExtension > state.PeakVwapExtension {
			state.PeakVwapExtension = currentExtension
		}

		// ⚡ TICK-LEVEL INVALIDATION STOP: Wrong-side penetration protection
		// (Keep evaluating against the live moving line, as crossing the actual live VWAP breaks the trend floor)
		maxAllowedCrossDistance := adrMultiplier * 2.0
		if currentSide == "LONG" && state.LatestPrice < state.LiveSessionVWAP && math.Abs(state.NormalizedVwapDistance) > maxAllowedCrossDistance {
			state.EntryVwapAnchor = 0.0 // Reset anchor memory
			e.mu.Unlock()
			return "EXIT_LONG"
		}
		if currentSide == "SHORT" && state.LatestPrice > state.LiveSessionVWAP && math.Abs(state.NormalizedVwapDistance) > maxAllowedCrossDistance {
			state.EntryVwapAnchor = 0.0 // Reset anchor memory
			e.mu.Unlock()
			return "EXIT_SHORT"
		}

		// Maintain Cash Currency Peak...
		if openTrade, trackingTrade := e.ActiveTrades[symbol]; trackingTrade {
			// Existing cash gain monitoring code...

			// ⚡ TICK-LEVEL PROTECTION EXIT EVALUATION
			if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
				state.EntryVwapAnchor = 0.0 // Clear out anchor memory on position collapse
				e.mu.Unlock()
				if trackingTrade {
					e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, state.LastUpdated)
				}
				return "EXIT_" + currentSide
			}
		}
	} else {
		// Clean up anchor memory explicitly whenever order metrics show position is FLAT
		state.EntryVwapAnchor = 0.0
	}

	e.mu.Unlock()
	return "HOLD"
}

// GenerateSignal handles execution tracking and logs freeze-frame microstructural metrics
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
		state.PeakVwapExtension = 0.0
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade

		currentExtension := math.Abs(state.NormalizedVwapDistance)
		if currentExtension > state.PeakVwapExtension {
			state.PeakVwapExtension = currentExtension
		}

		if openTrade, trackingTrade := e.ActiveTrades[symbol]; trackingTrade {
			multiplier := 1.0
			if openTrade.TradeSide == "SHORT" {
				multiplier = -1.0
			}
			currentCashPnL := (state.LatestPrice - averagePrice) * float64(netQty) * multiplier
			if currentCashPnL > openTrade.PeakPnLINR {
				openTrade.PeakPnLINR = currentCashPnL
			}
		}
	}

	e.mu.Unlock()

	// --- 🛑 TRACK A: FLAT EVALUATION (ENTRY DISCOVERY ROUTE) ---
	if state.CurrentSetupPhase == PhaseNeutral {
		e.mu.Lock()
		delete(e.ActiveTrades, symbol)
		e.mu.Unlock()

		signal := e.ActiveStrategy.CheckEntry(state)

		if signal == "GO_LONG" || signal == "GO_SHORT" {
			tradeSide := "LONG"
			if signal == "GO_SHORT" {
				tradeSide = "SHORT"
			}

			if len(state.BarHistory["1m"]) > 0 {
				latestBar := state.BarHistory["1m"][len(state.BarHistory["1m"])-1]
				state.LastTradedBarTime = latestBar.Timestamp
			}

			e.mu.Lock()
			// ⚓ LOCK THE FIXED ANCHOR VALUE HERE
			state.EntryVwapAnchor = state.LiveSessionVWAP

			e.ActiveTrades[symbol] = &OptimizationTradeLog{
				Symbol:            symbol,
				StrategyName:      e.ActiveStrategy.Name(),
				TradeSide:         tradeSide,
				MinutesSinceOpen:  state.ConsecutiveClosesAboveVwap + state.ConsecutiveClosesBelowVwap,
				EntryTimestamp:    state.LastUpdated,
				EntryPrice:        state.LatestPrice,
				EntryVwap:         state.LiveSessionVWAP,
				EntryVolumeRank:   state.LatestVolumeRank,
				EntryPriceRank:    state.LatestPriceRank,
				EntryVwapDistance: state.NormalizedVwapDistance,
			}
			e.mu.Unlock()
			return signal
		}
		return "HOLD"
	}

	// --- 🎯 TRACK B: ACTIVE POSITION MANAGEMENT (EXIT ROUTE) ---
	if state.CurrentSetupPhase == PhaseActiveTrade {
		e.mu.RLock()
		openTrade, trackingTrade := e.ActiveTrades[symbol]
		e.mu.RUnlock()

		marketExitTime := state.LastUpdated

		if e.ActiveStrategy.CheckTrailingProfitLock(state, currentSide) {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "INTELLIGENT_PROFIT_LOCK", averagePrice, netQty, marketExitTime)
			}
			return "EXIT_" + currentSide
		}

		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "STOP_LOSS", averagePrice, netQty, marketExitTime)
			}
			return "EXIT_" + currentSide
		}

		if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "TAKE_PROFIT", averagePrice, netQty, marketExitTime)
			}
			return "EXIT_" + currentSide
		}

		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			if trackingTrade {
				e.dispatchCompleteLog(symbol, openTrade, state.LatestPrice, "DIRECTION_FLIP", averagePrice, netQty, marketExitTime)
			}
			return signal
		}
	}

	return "HOLD"
}

func (e *Engine) dispatchCompleteLog(symbol string, trade *OptimizationTradeLog, exitPrice float64, reason string, avgPrice float64, qty int, exitTime time.Time) {
	e.mu.Lock()
	delete(e.ActiveTrades, symbol)
	e.mu.Unlock()

	trade.ExitTimestamp = exitTime
	trade.ExitPrice = exitPrice
	trade.ExitReason = reason

	multiplier := 1.0
	if trade.TradeSide == "SHORT" {
		multiplier = -1.0
	}

	trade.FinalPnLINR = (exitPrice - avgPrice) * float64(qty) * multiplier

	if e.OnTradeCompleted != nil {
		go e.OnTradeCompleted(trade)
	}
}
