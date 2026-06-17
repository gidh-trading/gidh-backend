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

// validateTimeAndCooldowns is the common helper function checking market times and order breaks.
// It returns (isAllowed, shouldAutoSquareOff, currentHM).
func (e *Engine) validateTimeAndCooldowns(state *InstrumentState, marketTime time.Time, isFlat bool) (bool, bool, int) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.Warnf("cannot load time location: %v", err)
		loc = time.UTC
	}

	istTime := marketTime.In(loc)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()

	// 1. 🛡️ BLOCK ALL TRADES BEFORE 9:30 AM IST
	if currentHM < 921 {
		return false, false, currentHM
	}

	cutoffHM := (AutoSquareOffHour * 100) + AutoSquareOffMinute

	// 2. 🛡️ HANDLE TIME CUTOFF AT OR AFTER 3:00 PM
	if currentHM >= cutoffHM {
		if !isFlat {
			// Signal an active position should be immediately auto-squared off
			return false, true, currentHM
		}
		// Block any new entry allocations
		return false, false, currentHM
	}

	// 3. 🛡️ ENFORCE 5-MINUTE COOLDOWN BREAK AFTER EXIT
	if isFlat && !state.LastExitSignalTime.IsZero() && marketTime.Sub(state.LastExitSignalTime) < 3*time.Minute {
		return false, false, currentHM
	}

	return true, false, currentHM
}

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
	state.LastTickTime = marketTime

	isFlatNow := currentSide == "FLAT" || currentSide == "" || state.CurrentSetupPhase == PhaseNeutral

	isAllowed, shouldSquareOff, _ := e.validateTimeAndCooldowns(state, marketTime, isFlatNow)

	if shouldSquareOff {
		// Real square-offs by the engine are executed actions, so we log them
		e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Auto_Square_Off_Hour_Reached", netQty, marketTime)
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		e.mu.Unlock()
		return "EXIT_" + currentSide
	}

	if !isAllowed {
		e.mu.Unlock()
		return "HOLD"
	}

	if state.CurrentSetupPhase == PhaseActiveTrade && (currentSide == "FLAT" || netQty == 0) {
		if marketTime.Sub(state.LastExitSignalTime) > 3*time.Second {
			logger.Warnf("⚠️ Asynchronous State Sync: Position for %s closed externally. Strategy will auto-heal on next tick.", symbol)
		}

		e.logStrategyDecision(state, symbol, "EXIT_"+state.ActiveSide, "External_Manual_Close_Detected", netQty, marketTime)
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.EntryTimestamp = time.Time{}
		state.LastExitSignalTime = marketTime
		isFlatNow = true
	}

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
		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			// ❌ STATE MUTATIONS REMOVED FROM HERE TO PREVENT LOGGING BLOCKED ENTRIES
			e.mu.Unlock()
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
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

	marketTime := state.LastTickTime
	isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0

	isAllowed, shouldSquareOff, _ := e.validateTimeAndCooldowns(state, marketTime, isFlatNow)

	if shouldSquareOff {
		e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Auto_Square_Off_Hour_Reached", netQty, marketTime)
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		e.mu.Unlock()
		return "EXIT_" + currentSide
	}

	if !isAllowed {
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

	if isFlatNow {
		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			// ❌ STATE MUTATIONS REMOVED FROM HERE TO PREVENT TRANSACTION VIOLATIONS
			e.mu.Unlock()
			return signal
		}

		e.mu.Unlock()
		return "HOLD"
	}

	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Stop_Loss", netQty, marketTime)
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		e.mu.Unlock()
		return "EXIT_" + currentSide
	}

	if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
		if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
			e.logStrategyDecision(state, symbol, "EXIT_"+currentSide, "Take_Profit", netQty, marketTime)
			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime
			e.mu.Unlock()
			return "EXIT_" + currentSide
		}
	}

	exitSignal := e.ActiveStrategy.CheckExit(state, currentSide)
	if exitSignal != "HOLD" && exitSignal != "" {
		e.logStrategyDecision(state, symbol, exitSignal, "Strategy_Exit", netQty, marketTime)
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		e.mu.Unlock()
		return exitSignal
	}

	e.mu.Unlock()
	return "HOLD"
}

func (e *Engine) CommitEntryTransaction(symbol string, signal string, netQty int, marketTime time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()

	state := e.getOrInitializeState(symbol)
	state.CurrentSetupPhase = PhaseActiveTrade
	state.EntryTimestamp = marketTime
	state.CurrentPnL = 0.0
	state.PeakPnL = 0.0

	e.logStrategyDecision(state, symbol, signal, "Strategy_Entry_Approved_By_Risk", netQty, marketTime)
}
