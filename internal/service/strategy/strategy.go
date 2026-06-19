package strategy

import (
	"fmt"
	"strings"
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
	stratConfigs map[string]*models.OptimizedStrategyConfig,
	dbW *writer.DBWriter,
) *Engine {
	masterStrat := NewVwapEfficiencyMomentumStrategy(stratConfigs)
	timeRouterWrapper := NewTimeBasedRouter(masterStrat)

	return &Engine{
		Registry:       make(map[string]*InstrumentState),
		ActiveStrategy: timeRouterWrapper,
		MaxBarLookback: barLookback,
		profiles:       profiles,
		dbWriter:       dbW,
	}
}

func (e *Engine) validateTimeAndCooldowns(state *InstrumentState, marketTime time.Time, isFlat bool) (bool, bool, int) {
	loc, err := time.LoadLocation("Asia/Kolkata")
	if err != nil {
		logger.Warnf("cannot load time location: %v", err)
		loc = time.UTC
	}

	istTime := marketTime.In(loc)
	currentHM := (istTime.Hour() * 100) + istTime.Minute()

	cutoffHM := (AutoSquareOffHour * 100) + AutoSquareOffMinute

	// 2. 🛡️ HANDLE TIME CUTOFF AT OR AFTER 3:00 PM
	if currentHM >= cutoffHM {
		if !isFlat {
			return false, true, currentHM // Signals shouldSquareOff = true to trigger panic exits
		}
		return false, false, currentHM
	}

	// 3. 🛡️ ENFORCE COOLDOWN BREAK AFTER EXIT (3 Minutes / 3 Candles breathing room)
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

// UpdateContext evaluates streaming tick context on an isolated copy. NO SIDE EFFECTS.
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) (string, *InstrumentState) {
	e.mu.Lock()
	realState := e.getOrInitializeState(enrichedTick.Raw.StockName)
	state := realState.Clone()
	e.mu.Unlock()

	marketTime := enrichedTick.Raw.Timestamp
	state.LatestPrice = enrichedTick.Raw.LastPrice
	state.ActiveSide = currentSide
	state.ActiveAvgPrice = averagePrice
	state.LastTickTime = marketTime

	isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0

	isAllowed, shouldSquareOff, _ := e.validateTimeAndCooldowns(state, marketTime, isFlatNow)
	if shouldSquareOff {
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		return "EXIT_" + currentSide, state
	}
	if !isAllowed {
		return "HOLD", state
	}

	// Handle continuous asynchronous external closure updates on the workspace snapshot
	if state.CurrentSetupPhase == PhaseActiveTrade && isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.EntryTimestamp = time.Time{}
		state.LastExitSignalTime = marketTime
	}

	if !isFlatNow {
		state.CurrentSetupPhase = PhaseActiveTrade
		qtyPos := float64(netQty)
		if qtyPos < 0 {
			qtyPos = -qtyPos
		}

		if currentSide == "LONG" {
			state.CurrentPnL = (state.LatestPrice - averagePrice) * qtyPos
		} else if currentSide == "SHORT" {
			state.CurrentPnL = (averagePrice - state.LatestPrice) * qtyPos
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

		// 1. Fetch the latest 1-minute bar context from history
		var latestBar *models.Bar
		if history, ok := state.BarHistory["1m"]; ok && len(history) > 0 {
			latestBar = history[len(history)-1]
		}

		signal := e.ActiveStrategy.CheckEntry(state, latestBar)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			return signal, state
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			return "EXIT_" + currentSide, state
		}
		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				return "EXIT_" + currentSide, state
			}
		}
		exitSignal := e.ActiveStrategy.CheckExit(state, currentSide)
		if exitSignal == "EXIT_LONG" || exitSignal == "EXIT_SHORT" {
			return exitSignal, state
		}
	}

	return "HOLD", state
}

// GenerateSignal calculates technical indications on top of an existing state context reference. NO SIDE EFFECTS.
func (e *Engine) GenerateSignal(symbol string, workingState *InstrumentState, currentSide string, averagePrice float64, netQty int) (string, *InstrumentState) {
	state := workingState.Clone()

	marketTime := state.LastTickTime
	isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0

	isAllowed, shouldSquareOff, _ := e.validateTimeAndCooldowns(state, marketTime, isFlatNow)
	if shouldSquareOff {
		state.CurrentSetupPhase = PhaseNeutral
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
		state.LastExitSignalTime = marketTime
		return "EXIT_" + currentSide, state
	}
	if !isAllowed {
		return "HOLD", state
	}

	// Update internal metadata structures matching target inputs
	if isFlatNow {
		state.CurrentSetupPhase = PhaseNeutral
		state.EntryVwapAnchor = 0.0
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
		if state.EntryVwapAnchor == 0 {
			state.EntryVwapAnchor = state.LiveSessionVWAP
		}
	}

	if currentSide != "FLAT" && currentSide != "" && netQty > 0 {
		qtyPos := float64(netQty)
		if qtyPos < 0 {
			qtyPos = -qtyPos
		}

		if currentSide == "LONG" {
			state.CurrentPnL = (state.LatestPrice - averagePrice) * qtyPos
		} else {
			state.CurrentPnL = (averagePrice - state.LatestPrice) * qtyPos
		}
		if state.CurrentPnL > state.PeakPnL {
			state.PeakPnL = state.CurrentPnL
		}
	}

	if isFlatNow && e.ActiveStrategy != nil {
		// 1. Fetch the latest 1-minute bar context from history
		var latestBar *models.Bar
		if history, ok := state.BarHistory["1m"]; ok && len(history) > 0 {
			latestBar = history[len(history)-1]
		}
		signal := e.ActiveStrategy.CheckEntry(state, latestBar)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			return signal, state
		}
		return "HOLD", state
	}

	if e.ActiveStrategy != nil {
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			return "EXIT_" + currentSide, state
		}
		if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				return "EXIT_" + currentSide, state
			}
		}
		exitSignal := e.ActiveStrategy.CheckExit(state, currentSide)
		if exitSignal != "HOLD" && exitSignal != "" {
			return exitSignal, state
		}
	}

	return "HOLD", state
}

// CommitTransaction safely pushes the fully derived final proposed state to global active memory.
func (e *Engine) CommitTransaction(symbol string, proposedState *InstrumentState, signal string, reason string, qty int) {
	e.mu.Lock()

	isEntry := signal == "GO_LONG" || signal == "GO_SHORT"
	isExit := strings.HasPrefix(signal, "EXIT_")

	if isEntry {
		proposedState.CurrentSetupPhase = PhaseActiveTrade
		proposedState.EntryTimestamp = proposedState.LastTickTime
		proposedState.EntryVwapAnchor = proposedState.LiveSessionVWAP
		proposedState.CurrentTradeID = fmt.Sprintf("TRD_%s_%d", symbol, proposedState.LastTickTime.UnixNano())
		proposedState.PeakPnL = 0.0
		proposedState.CurrentPnL = 0.0
		if e.ActiveStrategy != nil {
			e.ActiveStrategy.OnEntryCommit(proposedState, symbol)
		}

	} else if isExit {
		proposedState.CurrentSetupPhase = PhaseNeutral
		proposedState.LastExitSignalTime = proposedState.LastTickTime
	}

	e.Registry[symbol] = proposedState
	e.mu.Unlock()

	// 📊 Persist transaction histories explicitly on definitive Entry/Exit executions
	if isEntry || isExit {
		marketSnapshot := map[string]interface{}{
			"session_vwap":      proposedState.LiveSessionVWAP,
			"vwap_deviation":    proposedState.LatestPrice - proposedState.LiveSessionVWAP,
			"entry_vwap_anchor": proposedState.EntryVwapAnchor,
			"timeframe_phase":   proposedState.CurrentSetupPhase,
		}

		loggingTimeframe := "1m"
		if isExit {
			loggingTimeframe = "5m"
		}

		if history, ok := proposedState.BarHistory[loggingTimeframe]; ok && len(history) > 0 {
			latestBar := history[len(history)-1]
			marketSnapshot["bar_context"] = map[string]interface{}{
				"volume":  latestBar.Volume,
				"poc":     latestBar.POC,
				"vah":     latestBar.VAH,
				"val":     latestBar.VAL,
				"signals": latestBar.Analytics,
			}
		}

		txRecord := models.StrategyTransaction{
			TradeID:        proposedState.CurrentTradeID,
			StrategyName:   e.ActiveStrategy.Name(),
			Instrument:     strings.ToUpper(symbol),
			ActionType:     strings.ToUpper(signal),
			Price:          proposedState.LatestPrice,
			Quantity:       float64(qty),
			ExecutionTime:  proposedState.LastTickTime,
			TriggerReason:  reason,
			CurrentPnL:     proposedState.CurrentPnL,
			PeakPnL:        proposedState.PeakPnL,
			MarketSnapshot: marketSnapshot,
		}

		if e.dbWriter != nil {
			go e.dbWriter.PersistStrategyTransaction(txRecord)
		}
	}
}
