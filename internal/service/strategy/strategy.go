package strategy

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
)

const (
	AutoSquareOffHour   = 15 // 3 PM
	AutoSquareOffMinute = 0  // 00 minutes
)

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile
	dbWriter       *writer.DBWriter
}

func NewEngine(
	barLookback time.Duration,
	profiles map[string]*models.InstrumentProfile,
	dbW *writer.DBWriter,
) *Engine {
	masterStrat := NewVwapEfficiencyMomentumStrategy()
	timeRouterWrapper := NewTimeBasedRouter(masterStrat)

	return &Engine{
		Registry:       make(map[string]*InstrumentState),
		ActiveStrategy: timeRouterWrapper,
		MaxBarLookback: barLookback,
		profiles:       profiles,
		dbWriter:       dbW,
	}
}

// IngestClosedBar now reads the true market timestamp from the closed bar payload
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	e.mu.Lock()
	state := e.getOrInitializeState(bar.StockName)

	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP

	e.calculateActivePnLState(state, bar)
	e.appendAndPruneHistory(state, bar)
	e.mu.Unlock()
}

func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state := e.getOrInitializeState(symbol)
	marketTime := enrichedTick.Raw.Timestamp

	state.LatestPrice = enrichedTick.Raw.LastPrice
	state.ActiveSide = currentSide
	state.ActiveAvgPrice = averagePrice
	state.LastTickTime = marketTime // <-- Sync the latest market time for GenerateSignal to use

	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.Warnf("cannot load time location: %v", err)
		loc = time.UTC
	}

	istTime := marketTime.In(loc)

	// 1. TIME CUTOFF CHECK (Strategy Layer)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()

	// 🛡️ BLOCK ALL TRADES BEFORE 9:30 AM IST
	if currentHM < 930 {
		e.mu.Unlock()
		return "HOLD"
	}

	cutoffHM := (AutoSquareOffHour * 100) + AutoSquareOffMinute

	if currentHM >= cutoffHM {
		if currentSide != "FLAT" && currentSide != "" {
			// Log the forced time cutoff square off event before resetting engine tracking values
			e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Auto_Square_Off_Hour_Reached", netQty, marketTime)

			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime
			e.mu.Unlock()
			return "EXIT_" + currentSide
		}
	}

	// 2. MANUAL CLOSE DETECTION
	if state.CurrentSetupPhase == PhaseActiveTrade && (currentSide == "FLAT" || netQty == 0) {
		if marketTime.Sub(state.LastExitSignalTime) > 3*time.Second {
			logger.Warnf("⚠️ Asynchronous State Sync: Position for %s closed externally. Strategy will auto-heal on next tick.", symbol)
		}

		// Log the manual closing detection for historical integrity anomalies matching strategy rules
		e.logStrategyDecision(state, symbol, "EXIT_"+state.ActiveSide, "External_Manual_Close_Detected", netQty, marketTime)

		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.EntryTimestamp = marketTime
		// If closed manually, also enforce the 5-minute break from right now
		state.LastExitSignalTime = marketTime
	}

	isFlatNow := currentSide == "FLAT" || currentSide == "" || state.CurrentSetupPhase == PhaseNeutral

	if !isFlatNow && state.CurrentSetupPhase != PhaseActiveTrade {
		state.CurrentSetupPhase = PhaseActiveTrade
	}

	if !isFlatNow {
		if currentSide == "LONG" {
			state.CurrentPnL = state.LatestPrice - averagePrice
		} else if currentSide == "SHORT" {
			state.CurrentPnL = averagePrice - state.LatestPrice
		}

		if state.CurrentPnL > state.PeakPnL {
			state.PeakPnL = state.CurrentPnL
		}
	} else {
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
	}

	if isFlatNow && e.ActiveStrategy != nil {
		// 3. BLOCK NEW ENTRIES AFTER CUTOFF
		if currentHM >= cutoffHM {
			e.mu.Unlock()
			return "HOLD"
		}

		// 4. NEW: 5-MINUTE BREAK AFTER A TRADE FINISHES
		if !state.LastExitSignalTime.IsZero() && marketTime.Sub(state.LastExitSignalTime) < 10*time.Minute {
			e.mu.Unlock()
			return "HOLD"
		}

		// Enforce the 1-minute cooldown from the last entry (to prevent double entries)
		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) < 1*time.Minute {
			e.mu.Unlock()
			return "HOLD"
		}

		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			state.CurrentSetupPhase = PhaseActiveTrade
			state.EntryTimestamp = marketTime

			// Initialize PnL tracking nodes fresh on allocation sequence initialization
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0

			// Log entry rules transaction matching details
			e.logStrategyDecision(state, symbol, signal, "Strategy_Entry_Condition_Met", netQty, marketTime)

			e.mu.Unlock()
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			// Log the exit transaction BEFORE clearing the tracking state
			e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Stop_Loss_Hit", netQty, marketTime)

			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime
			e.mu.Unlock()
			return "EXIT_" + currentSide
		}

		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				// Log the profit target execution record BEFORE state clearance metrics drop
				e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Take_Profit_Hit", netQty, marketTime)

				state.CurrentSetupPhase = PhaseNeutral
				state.CurrentPnL = 0.0
				state.PeakPnL = 0.0
				state.LastExitSignalTime = marketTime
				e.mu.Unlock()
				return "EXIT_" + currentSide
			}
		}

		exitSignal := e.ActiveStrategy.CheckExit(state, currentSide)
		if exitSignal == "EXIT_LONG" || exitSignal == "EXIT_SHORT" {
			e.logStrategyDecision(state, symbol, exitSignal, "Strategy_Condition_Exit", netQty, marketTime)

			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime
			e.mu.Unlock()
			return exitSignal
		}
	}

	e.mu.Unlock()
	return "HOLD"
}

func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)

	if currentSide != "FLAT" && currentSide != "" && netQty > 0 {
		if currentSide == "LONG" {
			state.CurrentPnL = state.LatestPrice - averagePrice
		} else {
			state.CurrentPnL = averagePrice - state.LatestPrice
		}
		if state.CurrentPnL > state.PeakPnL {
			state.PeakPnL = state.CurrentPnL
		}
	}
	marketTime := state.LastTickTime // Capture time before unlock
	e.mu.Unlock()

	isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0
	if isFlatNow {
		if !state.LastExitSignalTime.IsZero() && state.LastTickTime.Sub(state.LastExitSignalTime) < 5*time.Minute {
			return "HOLD"
		}

		signal := e.ActiveStrategy.CheckEntry(state)
		// FIX: Log the entry!
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			e.logStrategyDecision(state, symbol, signal, "Strategy_Entry", netQty, marketTime)
		}
		return signal
	}

	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		// FIX: Log Stop Loss!
		e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Stop_Loss", netQty, marketTime)
		return "EXIT_" + currentSide
	}
	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		// FIX: Log Take Profit!
		e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Take_Profit", netQty, marketTime)
		return "EXIT_" + currentSide
	}

	exitSignal := e.ActiveStrategy.CheckExit(state, currentSide)
	// FIX: Log standard Strategy Exit!
	if exitSignal != "HOLD" {
		e.logStrategyDecision(state, symbol, exitSignal, "Strategy_Exit", netQty, marketTime)
	}

	return exitSignal
}
