package strategy

import (
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

const (
	EfficiencySlopeLookback = 10
)

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile
	dbWriter       *writer.DBWriter

	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog)
}

func NewEngine(
	barLookback time.Duration,
	profiles map[string]*models.InstrumentProfile,
	dbW *writer.DBWriter,
	completeHook func(log *OptimizationTradeLog),
) *Engine {
	ledgerStrategyCard := NewInstitutionalLedgerStrategy()
	timeRouterWrapper := NewTimeBasedRouter(ledgerStrategyCard)

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   timeRouterWrapper,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		dbWriter:         dbW,
		OnTradeCompleted: completeHook,
	}
}

// EnrichLiveBar Enriches live tick streaming data payload right before transfer
func (e *Engine) EnrichLiveBar(bar *models.Bar) {
	e.mu.RLock()
	state, exists := e.Registry[bar.StockName]
	if !exists {
		e.mu.RUnlock()
		return
	}
	netEff := state.NetEfficiency
	slope := state.NetEfficiencySlope

	// Dynamically calculate the signed distance on current price frame
	vwapDist := e.calculateNormalizedDistance(bar.Close, bar.VWAP, state.Profile)
	e.mu.RUnlock()

	bar.Analytics.NetEfficiency = netEff
	bar.Analytics.NetEfficiencySlope = slope
	bar.Analytics.NormalizedVwapDistance = vwapDist
}

// IngestClosedBar Tracks and saves metric snapshot arrays when a bar close frame triggers
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	e.mu.Lock()
	state := e.getOrInitializeState(bar.StockName)

	e.updateCoreBarMetrics(state, bar)
	e.trackVwapAcceptance(state, bar)

	state.TotalSessionBars++
	if bar.Close > bar.VWAP {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1)) + 1.0
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	} else {
		aboveCount := (state.TimePctAboveVwap * float64(state.TotalSessionBars-1))
		state.TimePctAboveVwap = aboveCount / float64(state.TotalSessionBars)
	}

	e.ProcessClosedBarLedger(state, bar)

	state.NetEfficiency = state.Ledger.BullEfficient - state.Ledger.BearEfficient
	state.NetEfficiencyHistory = append(state.NetEfficiencyHistory, state.NetEfficiency)

	if len(state.NetEfficiencyHistory) > EfficiencySlopeLookback {
		state.NetEfficiencyHistory = state.NetEfficiencyHistory[1:]
	}

	state.NetEfficiencySlope = CalculateLinearRegressionSlope(state.NetEfficiencyHistory)

	// --- 📈 PNL & EFFICIENCY METRIC TRACING ON BAR CLOSE ---
	if state.CurrentSetupPhase == PhaseActiveTrade {
		if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists {
			var currentUnrealized float64
			if tradeLog.TradeSide == "LONG" {
				currentUnrealized = (bar.Close - tradeLog.EntryPrice)
			} else if tradeLog.TradeSide == "SHORT" {
				currentUnrealized = (tradeLog.EntryPrice - bar.Close)
			}

			state.CurrentPnL = currentUnrealized
			if state.CurrentPnL > state.PeakPnL {
				state.PeakPnL = state.CurrentPnL
			}

			if state.PeakPnL > tradeLog.PeakPnLINR {
				tradeLog.PeakPnLINR = state.PeakPnL
			}

			if tradeLog.TradeSide == "LONG" {
				if state.NetEfficiency > state.PeakEfficiency {
					state.PeakEfficiency = state.NetEfficiency
				}
			} else if tradeLog.TradeSide == "SHORT" {
				absEff := math.Abs(state.NetEfficiency)
				if state.NetEfficiency < 0 && absEff > state.PeakEfficiency {
					state.PeakEfficiency = absEff
				}
			}
		}
	} else {
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
	}

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	bar.Analytics.NetEfficiency = state.NetEfficiency
	bar.Analytics.NetEfficiencySlope = state.NetEfficiencySlope
	bar.Analytics.NormalizedVwapDistance = state.NormalizedVwapDistance

	// --- 🛡️ EVALUATE STRUCTURAL EXITS AND TAKE PROFITS AT BAR CLOSE ---
	if state.CurrentSetupPhase == PhaseActiveTrade && e.ActiveStrategy != nil {
		currentSide := "LONG"
		var avgPrice float64
		if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists {
			if tradeLog.TradeSide == "SHORT" {
				currentSide = "SHORT"
			}
			avgPrice = tradeLog.EntryPrice
		} else {
			avgPrice = state.LatestPrice
		}

		// ✅ FIX 2: Stop Loss evaluation integrated
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, avgPrice, 1) {
			e.mu.Unlock()
			e.LogOptimizationExit(bar.StockName, "STOP_LOSS", state)
			e.mu.Lock()
		} else if e.ActiveStrategy.CheckTakeProfit(state, currentSide, avgPrice, 1) {
			e.mu.Unlock()
			e.LogOptimizationExit(bar.StockName, "TAKE_PROFIT_BAR_CLOSE", state)
			e.mu.Lock()
		} else {
			signal := e.ActiveStrategy.CheckExit(state, currentSide)
			if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
				e.mu.Unlock()
				e.LogOptimizationExit(bar.StockName, signal, state)
				e.mu.Lock()
			}
		}
	}

	e.appendAndPruneHistory(state, bar)
	e.mu.Unlock()

	if e.dbWriter != nil {
		e.dbWriter.AddBar(*bar)
	}
}

// UpdateContext processes ticks and restricts live actions exclusively to entries and immediate price safety checks
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state := e.getOrInitializeState(symbol)

	e.updateCoreTickMetrics(state, enrichedTick.Raw)
	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)
	state.NetEfficiencySlope = CalculateLinearRegressionSlope(state.NetEfficiencyHistory)

	isFlatNow := currentSide == "FLAT" || currentSide == "" || state.CurrentSetupPhase == PhaseNeutral

	if !isFlatNow && state.CurrentSetupPhase != PhaseActiveTrade {
		state.CurrentSetupPhase = PhaseActiveTrade
	}

	// --- 📈 REAL-TIME TICK PNL TRACING ---
	if !isFlatNow {
		var currentUnrealized float64
		if currentSide == "LONG" {
			currentUnrealized = state.LatestPrice - averagePrice
		} else if currentSide == "SHORT" {
			currentUnrealized = averagePrice - state.LatestPrice
		}

		state.CurrentPnL = currentUnrealized
		if state.CurrentPnL > state.PeakPnL {
			state.PeakPnL = state.CurrentPnL
		}

		if tradeLog, exists := e.ActiveTrades[symbol]; exists {
			if state.PeakPnL > tradeLog.PeakPnLINR {
				tradeLog.PeakPnLINR = state.PeakPnL
			}
		}
	} else {
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
	}

	// --- 🟢 ENTRY EXECUTION WITH CONCURRENCY LOCKS ---
	if isFlatNow && e.ActiveStrategy != nil {
		if _, duplicateActive := e.ActiveTrades[symbol]; duplicateActive {
			e.mu.Unlock()
			return "HOLD"
		}

		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			state.CurrentSetupPhase = PhaseActiveTrade

			// ✅ FIX 1: Safely write entry while lock is maintained using locked variant
			e.logOptimizationEntryLocked(symbol, signal, state)

			e.mu.Unlock()
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		// --- 🔴 RISK TICK MANAGEMENT ---
		tradeLog, tradeExists := e.ActiveTrades[symbol]

		// ✅ FIX 2: Check Stop Loss on every live tick to prevent catastrophic bleed
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			e.mu.Unlock()
			e.LogOptimizationExit(symbol, "STOP_LOSS", state)
			return "EXIT_" + currentSide
		}

		if tradeExists && time.Since(tradeLog.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				e.mu.Unlock()
				e.LogOptimizationExit(symbol, "TAKE_PROFIT_TRAILING_TICK", state)
				return "EXIT_" + currentSide
			}
		}

		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			// ✅ FIX 4: Removed the restrictive VWAP blocker. Exit triggers purely on signal logic.
			e.mu.Unlock()
			e.LogOptimizationExit(symbol, signal, state)
			return signal
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
			state.CurrentPnL = (state.LatestPrice - averagePrice)
		} else {
			state.CurrentPnL = (averagePrice - state.LatestPrice)
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

	// ✅ FIX 2: Include Stop Loss within generator signal block
	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}

	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}

	return e.ActiveStrategy.CheckExit(state, currentSide)
}

// LogOptimizationEntry Wraps the locked entry tracking function for external calls
func (e *Engine) LogOptimizationEntry(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logOptimizationEntryLocked(symbol, signal, state)
}

// logOptimizationEntryLocked Internally safely records trade entries while Engine Mutex is held
func (e *Engine) logOptimizationEntryLocked(symbol string, signal string, state *InstrumentState) {
	if _, exists := e.ActiveTrades[symbol]; exists {
		return
	}

	tradeSide := "LONG"
	if signal == "GO_SHORT" {
		tradeSide = "SHORT"
	}

	strategyName := "Institutional_Ledger"
	if e.ActiveStrategy != nil {
		strategyName = e.ActiveStrategy.Name()
	}

	state.PeakEfficiency = 0.0
	state.CurrentPnL = 0.0
	state.PeakPnL = 0.0
	state.CurrentSetupPhase = PhaseActiveTrade

	historyLength := len(state.NetEfficiencyHistory)
	var entryDelta float64
	if historyLength >= 2 {
		entryDelta = state.NetEfficiency - state.NetEfficiencyHistory[historyLength-2]
	}

	log := &OptimizationTradeLog{
		Symbol:            symbol,
		StrategyName:      strategyName,
		TradeSide:         tradeSide,
		EntryTimestamp:    time.Now(),
		EntryPrice:        state.LatestPrice,
		EntryVwap:         state.LiveSessionVWAP,
		EntryVwapDistance: state.NormalizedVwapDistance,
		EntryEfficiency:   state.NetEfficiency,
		EntryDelta:        entryDelta,
		EntrySlope:        state.NetEfficiencySlope,
		EntryVolumeRank:   state.LatestVolumeRank,
		CreatedAt:         time.Now(),
	}

	e.ActiveTrades[symbol] = log
}

func (e *Engine) LogOptimizationExit(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	tradeLog, exists := e.ActiveTrades[symbol]
	if !exists {
		// 🛡️ Safe Concurrency Intercept
		e.mu.Unlock()
		return
	}

	// Synchronously delete the active trade record inside the active lock boundary.
	delete(e.ActiveTrades, symbol)

	// Clean up position state metrics synchronously
	state.CurrentSetupPhase = PhaseNeutral
	state.PeakEfficiency = 0.0
	state.CurrentPnL = 0.0
	state.PeakPnL = 0.0
	e.mu.Unlock()

	// --- 📈 CALCULATE TRADE OVERALL PERFORMANCE LOG ---
	tradeLog.ExitTimestamp = time.Now()
	tradeLog.ExitPrice = state.LatestPrice
	tradeLog.ExitReason = signal

	var finalPnL float64
	if tradeLog.TradeSide == "LONG" {
		finalPnL = tradeLog.ExitPrice - tradeLog.EntryPrice
	} else {
		finalPnL = tradeLog.EntryPrice - tradeLog.ExitPrice
	}
	tradeLog.FinalPnLINR = finalPnL

	if tradeLog.PeakPnLINR > 0 {
		tradeLog.EfficiencyCaptureRatio = finalPnL / tradeLog.PeakPnLINR
	} else if tradeLog.PeakPnLINR == 0 && finalPnL == 0 {
		tradeLog.EfficiencyCaptureRatio = 1.0
	} else {
		tradeLog.EfficiencyCaptureRatio = -1.0
	}

	// ✅ FIX: Removed the concurrent db.LogStrategyOptimizationTradeExpanded goroutine.
	// The double-write occurred because e.OnTradeCompleted also handles saving the order.
	// Relying on the single pass-through hook ensures perfect synchronicity.
	if e.OnTradeCompleted != nil {
		e.OnTradeCompleted(tradeLog)
	}
}
