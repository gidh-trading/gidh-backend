package strategy

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

type Engine struct {
	mu              sync.RWMutex
	Registry        map[string]*InstrumentState // Key: symbol_strategyName
	ActiveRouter    *TimeBasedRouter
	MaxBarLookback  time.Duration
	profiles        map[string]*models.InstrumentProfile
	vwapPercentiles map[string]*models.VWAPDistancePercentile
	dbWriter        *writer.DBWriter
}

func NewEngine(
	barLookback time.Duration,
	profiles map[string]*models.InstrumentProfile,
	vwapPercentiles map[string]*models.VWAPDistancePercentile,
	dbW *writer.DBWriter,
) *Engine {
	return &Engine{
		Registry:        make(map[string]*InstrumentState),
		ActiveRouter:    NewTimeBasedRouter(),
		MaxBarLookback:  barLookback,
		profiles:        profiles,
		vwapPercentiles: vwapPercentiles,
		dbWriter:        dbW,
	}
}

// IngestClosedBar broadcasts incoming OHLC history blocks across all matching strategies
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	e.mu.Lock()
	defer e.mu.Unlock()

	symbol := bar.StockName

	// Update records for all strategies tracking this particular symbol
	for name := range e.ActiveRouter.GetStrategies() {
		state := e.getOrInitializeState(symbol, name)

		state.LatestPrice = bar.Close
		state.LiveSessionVWAP = bar.VWAP

		if bar.Timeframe == "1m" {
			if state.SessionOpen == 0 {
				state.SessionOpen = bar.Open
				state.SessionHigh = bar.High
				state.SessionLow = bar.Low
			} else {
				if bar.High > state.SessionHigh {
					state.SessionHigh = bar.High
				}
				if bar.Low < state.SessionLow {
					state.SessionLow = bar.Low
				}
			}
		}

		ceilingPrice, floorPrice, ok := e.GetADRBounds(state)
		if ok {
			state.ADRHigh = ceilingPrice
			state.ADRLow = floorPrice
		}

		e.calculateActivePnLState(state, bar)
		e.appendAndPruneHistory(state, bar)
	}
}

// UpdateContext evaluates live streaming ticks concurrently over all isolated strategy sandboxes
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) map[string]TickResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	symbol := enrichedTick.Raw.StockName
	marketTime := enrichedTick.Raw.Timestamp
	results := make(map[string]TickResult)

	for name, strat := range e.ActiveRouter.GetStrategies() {
		realState := e.getOrInitializeState(symbol, name)
		state := realState.Clone()

		state.LatestPrice = enrichedTick.Raw.LastPrice
		state.ActiveSide = currentSide
		state.ActiveAvgPrice = averagePrice
		state.LastTickTime = marketTime

		isFlatNow := currentSide == "FLAT" || currentSide == "" || netQty == 0

		isAllowed, shouldSquareOff := e.ActiveRouter.ValidateTimeAndCooldowns(strat, state, marketTime, isFlatNow)
		if shouldSquareOff {
			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.LastExitSignalTime = marketTime
			results[name] = TickResult{Signal: "EXIT_" + currentSide, State: state}
			continue
		}

		if !isAllowed {
			results[name] = TickResult{Signal: "HOLD", State: state}
			continue
		}

		// Sync asynchronous platform execution events
		if state.CurrentSetupPhase == PhaseActiveTrade && isFlatNow {
			state.CurrentSetupPhase = PhaseNeutral
			state.CurrentPnL = 0.0
			state.PeakPnL = 0.0
			state.EntryTimestamp = time.Time{}
			state.LastExitSignalTime = marketTime
		}

		if !isFlatNow {
			state.CurrentSetupPhase = PhaseActiveTrade
			qtyPos := mathAbs(float64(netQty))

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

		// Dynamic Signal Evaluation
		if isFlatNow {
			signal := strat.CheckEntry(state)
			results[name] = TickResult{Signal: signal, State: state}
		} else {
			if strat.CheckStopLoss(state, currentSide, averagePrice, netQty, e.profiles) {
				results[name] = TickResult{Signal: "EXIT_" + currentSide, State: state}
				continue
			}
			if !state.EntryTimestamp.IsZero() && marketTime.Sub(state.EntryTimestamp) > 1*time.Minute {
				// PASS e.vwapPercentiles HERE
				if strat.CheckTakeProfit(state, currentSide, averagePrice, netQty, e.vwapPercentiles) {
					results[name] = TickResult{Signal: "EXIT_" + currentSide, State: state}
					continue
				}
			}
			exitSignal := strat.CheckExit(state, currentSide)
			if exitSignal == "EXIT_LONG" || exitSignal == "EXIT_SHORT" {
				results[name] = TickResult{Signal: exitSignal, State: state}
			} else {
				results[name] = TickResult{Signal: "HOLD", State: state}
			}
		}
	}

	return results
}

// CommitTransaction safely pushes a proposed state modification to active tracking memory
func (e *Engine) CommitTransaction(symbol string, strategyName string, proposedState *InstrumentState, signal string, reason string, qty int) {
	e.mu.Lock()
	compositeKey := fmt.Sprintf("%s_%s", symbol, strategyName)

	isEntry := signal == "GO_LONG" || signal == "GO_SHORT"
	isExit := strings.HasPrefix(signal, "EXIT_")

	if isEntry {
		proposedState.CurrentSetupPhase = PhaseActiveTrade
		proposedState.EntryTimestamp = proposedState.LastTickTime
		proposedState.EntryVwapAnchor = proposedState.LiveSessionVWAP
		proposedState.CurrentTradeID = fmt.Sprintf("TRD_%s_%d", symbol, proposedState.LastTickTime.UnixNano())
		proposedState.PeakPnL = 0.0
		proposedState.CurrentPnL = 0.0

		// Extract current strategy metric block
		stats := proposedState.StrategyHistory[strategyName]
		stats.TradeCount++
		stats.LastTradeTime = proposedState.LastTickTime
		stats.IsCurrentlyActive = true
		proposedState.StrategyHistory[strategyName] = stats

		if strat, ok := e.ActiveRouter.GetStrategies()[strategyName]; ok {
			strat.OnEntryCommit(proposedState, symbol)
		}

	} else if isExit {
		proposedState.CurrentSetupPhase = PhaseNeutral
		proposedState.LastExitSignalTime = proposedState.LastTickTime

		stats := proposedState.StrategyHistory[strategyName]
		stats.IsCurrentlyActive = false
		proposedState.StrategyHistory[strategyName] = stats
	}

	e.Registry[compositeKey] = proposedState
	e.mu.Unlock()

	// 📊 Persist execution records asynchronously
	if (isEntry || isExit) && e.dbWriter != nil {
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
			StrategyName:   strategyName,
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

		go e.dbWriter.PersistStrategyTransaction(txRecord)
	}
}
