package strategy

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
)

type Engine struct {
	mu             sync.RWMutex
	Registry       map[string]*InstrumentState
	ActiveStrategy Strategy
	MaxBarLookback time.Duration
	profiles       map[string]*models.InstrumentProfile

	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog)
}

func NewEngine(
	barLookback time.Duration,
	profiles map[string]*models.InstrumentProfile,
	completeHook func(log *OptimizationTradeLog),
) *Engine {
	testStrategyCard := NewVwapEfficiencyReversalStrategy()
	timeRouterWrapper := NewTimeBasedRouter(testStrategyCard)

	return &Engine{
		Registry:         make(map[string]*InstrumentState),
		ActiveTrades:     make(map[string]*OptimizationTradeLog),
		ActiveStrategy:   timeRouterWrapper,
		MaxBarLookback:   barLookback,
		profiles:         profiles,
		OnTradeCompleted: completeHook,
	}
}

// IngestClosedBar handles clean, thread-safe trade execution rules from fully calculated bar payloads
func (e *Engine) IngestClosedBar(bar *models.Bar) {
	e.mu.Lock()
	state := e.getOrInitializeState(bar.StockName)

	// 1. Snapshot running price parameters straight from the incoming fully-analyzed bar
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	// Decorate outgoing bar analytical layout for reference consistency
	bar.Analytics.NormalizedVwapDistance = state.NormalizedVwapDistance

	// 2. Track unrealized open position portfolio performance metrics
	e.calculateActivePnLState(state, bar)

	// 3. Pure mechanical risk exit checks (Stop Loss & Trailing Safeguard)
	e.evaluateExecutionRiskSafely(state, bar)

	// 4. Record bar into historical timeframe lookup buffer for Strategy Module evaluations
	e.appendAndPruneHistory(state, bar)
	e.mu.Unlock()
}

// UpdateContext evaluates tick-level structural risk adjustments and entry gateways
func (e *Engine) UpdateContext(enrichedTick *models.EnrichedTick, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	symbol := enrichedTick.Raw.StockName
	state := e.getOrInitializeState(symbol)

	// Live context sync
	state.LatestPrice = enrichedTick.Raw.LastPrice
	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

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
		if tradeLog, exists := e.ActiveTrades[symbol]; exists {
			if state.PeakPnL > tradeLog.PeakPnLINR {
				tradeLog.PeakPnLINR = state.PeakPnL
			}
		}
	} else {
		state.CurrentPnL = 0.0
		state.PeakPnL = 0.0
	}

	// --- ENTRY EXECUTION BLOCK ---
	if isFlatNow && e.ActiveStrategy != nil {
		if _, duplicateActive := e.ActiveTrades[symbol]; duplicateActive {
			e.mu.Unlock()
			return "HOLD"
		}

		// ENGINE CLUSTERING GATEKEEPER:
		// Scan active trades. If an entry occurred within 60s anywhere, bypass execution
		for _, activeTrade := range e.ActiveTrades {
			if time.Since(activeTrade.EntryTimestamp) < 1*time.Minute {
				e.mu.Unlock()
				return "HOLD"
			}
		}

		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			state.CurrentSetupPhase = PhaseActiveTrade
			e.logOptimizationEntryLocked(symbol, signal, state)
			e.mu.Unlock()
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		// --- RISK TICK SAFETY DISPATCHER ---
		if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
			e.mu.Unlock()
			go e.LogOptimizationExit(symbol, "SAFETY_STOP_LOSS", state)
			return "EXIT_" + currentSide
		}

		tradeLog, tradeExists := e.ActiveTrades[symbol]
		if tradeExists && time.Since(tradeLog.EntryTimestamp) > 1*time.Minute {
			if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
				e.mu.Unlock()
				go e.LogOptimizationExit(symbol, "SAFETY_HIGH_CONF_TRAILING", state)
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

	if e.ActiveStrategy.CheckStopLoss(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}

	if e.ActiveStrategy.CheckTakeProfit(state, currentSide, averagePrice, netQty) {
		return "EXIT_" + currentSide
	}

	return e.ActiveStrategy.CheckExit(state, currentSide)
}

func (e *Engine) LogOptimizationEntry(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.logOptimizationEntryLocked(symbol, signal, state)
}

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

	state.CurrentPnL = 0.0
	state.PeakPnL = 0.0
	state.CurrentSetupPhase = PhaseActiveTrade

	// Extract context dynamically from the latest cached closed bars if available
	var entryEff, entrySlope float64
	if state.BarHistory != nil {
		// Fallback lookup defaults against standard "1m" tracking frames
		if history, ok := state.BarHistory["1m"]; ok && len(history) > 0 {
			lastBar := history[len(history)-1]
			entryEff = lastBar.Analytics.NetEfficiency
			entrySlope = lastBar.Analytics.NetEfficiencySlope
		}
	}

	log := &OptimizationTradeLog{
		Symbol:            symbol,
		StrategyName:      strategyName,
		TradeSide:         tradeSide,
		EntryTimestamp:    time.Now(),
		EntryPrice:        state.LatestPrice,
		EntryVwap:         state.LiveSessionVWAP,
		EntryVwapDistance: state.NormalizedVwapDistance,
		EntryEfficiency:   entryEff,
		EntrySlope:        entrySlope,
		CreatedAt:         time.Now(),
	}

	e.ActiveTrades[symbol] = log
}

func (e *Engine) LogOptimizationExit(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	tradeLog, exists := e.ActiveTrades[symbol]
	if !exists {
		e.mu.Unlock()
		return
	}

	delete(e.ActiveTrades, symbol)

	state.CurrentSetupPhase = PhaseNeutral
	state.CurrentPnL = 0.0
	state.PeakPnL = 0.0
	e.mu.Unlock()

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

	if e.OnTradeCompleted != nil {
		e.OnTradeCompleted(tradeLog)
	}
}
