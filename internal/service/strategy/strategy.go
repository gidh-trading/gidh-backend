package strategy

import (
	"gidh-backend/pkg/logger"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
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

	// 1. TIME CUTOFF CHECK (Strategy Layer)
	currentHM := (marketTime.Hour() * 100) + marketTime.Minute()
	cutoffHM := (AutoSquareOffHour * 100) + AutoSquareOffMinute

	if currentHM >= cutoffHM {
		// If we have an active position past cutoff time, force an exit.
		if currentSide != "FLAT" && currentSide != "" {
			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime // <-- Record exit time
			e.mu.Unlock()
			return "EXIT_" + currentSide // RiskManager will execute this
		}
	}

	// 2. MANUAL CLOSE DETECTION
	// If the strategy thought we were in an active trade, but the incoming tick reports we are FLAT (netQty == 0)
	if state.CurrentSetupPhase == PhaseActiveTrade && (currentSide == "FLAT" || netQty == 0) {

		// Only trigger the warning if it has been more than 3 seconds since our last algorithmic exit signal
		if marketTime.Sub(state.LastExitSignalTime) > 3*time.Second {
			logger.Warnf("⚠️ Asynchronous State Sync: Position for %s closed externally. Strategy will auto-heal on next tick.", symbol)
		}

		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		// Setting the EntryTimestamp to now enforces the existing 1-minute cooldown,
		// preventing the bot from instantly re-entering the trade you just closed.
		state.EntryTimestamp = marketTime
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

		// Enforce the 1-minute cooldown from the last entry (or manual exit)
		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) < 1*time.Minute {
			e.mu.Unlock()
			return "HOLD"
		}

		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			state.CurrentSetupPhase = PhaseActiveTrade
			state.EntryTimestamp = marketTime
			e.mu.Unlock()
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime // <-- Record exit time
			e.mu.Unlock()
			return "EXIT_" + currentSide
		}

		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				state.CurrentSetupPhase = PhaseNeutral
				state.CurrentPnL = 0.0
				state.PeakPnL = 0.0
				state.LastExitSignalTime = marketTime // <-- Record exit time
				e.mu.Unlock()
				return "EXIT_" + currentSide
			}
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
	e.mu.Unlock()

	isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0
	if isFlatNow {
		return e.ActiveStrategy.CheckEntry(state)
	}
	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}
	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}
	return e.ActiveStrategy.CheckExit(state, currentSide)
}
