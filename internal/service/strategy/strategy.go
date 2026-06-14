package strategy

import (
	"context"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
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

// IngestClosedBar handles structural bar transitions and evaluates strategy exits at bar close
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

	// Update active trade peak variables exclusively at bar close
	if state.CurrentSetupPhase == PhaseActiveTrade {
		if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists {
			var currentUnrealized float64
			if tradeLog.TradeSide == "LONG" {
				currentUnrealized = (bar.Close - tradeLog.EntryPrice)
			} else if tradeLog.TradeSide == "SHORT" {
				currentUnrealized = (tradeLog.EntryPrice - bar.Close)
			}
			if currentUnrealized > tradeLog.PeakPnLINR {
				tradeLog.PeakPnLINR = currentUnrealized
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
	}

	bar.Analytics.NetEfficiency = state.NetEfficiency
	bar.Analytics.NetEfficiencySlope = state.NetEfficiencySlope

	state.NormalizedVwapDistance = e.calculateNormalizedDistance(state.LatestPrice, state.LiveSessionVWAP, state.Profile)

	// --- 🛡️ FIX CHURN: EVALUATE STRUCTURAL EXITS EXCLUSIVELY AT BAR CLOSE ---
	// If an active trade is open, check if microstructural decay or overextensions warrant an exit
	if state.CurrentSetupPhase == PhaseActiveTrade && e.ActiveStrategy != nil {
		// Temporary side lookup mock matching internal engine side properties
		currentSide := "LONG"
		if tradeLog, exists := e.ActiveTrades[bar.StockName]; exists && tradeLog.TradeSide == "SHORT" {
			currentSide = "SHORT"
		}

		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			e.mu.Unlock() // Unlock before log persistence workers execute
			e.LogOptimizationExit(bar.StockName, signal, state)
			e.mu.Lock()
		}
	}
	// ------------------------------------------------------------------------

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

	isFlatNow := currentSide == "FLAT" || currentSide == ""

	if isFlatNow && e.ActiveStrategy != nil {
		signal := e.ActiveStrategy.CheckEntry(state)
		if signal == "GO_LONG" || signal == "GO_SHORT" {
			e.mu.Unlock()
			e.LogOptimizationEntry(symbol, signal, state)
			return signal
		}
	} else if !isFlatNow && e.ActiveStrategy != nil {
		// Ticks are ONLY allowed to verify core price invalidations (e.g., crossing VWAP corridors)
		// Microstructural decay filters are skipped here and deferred to IngestClosedBar
		signal := e.ActiveStrategy.CheckExit(state, currentSide)
		if signal == "EXIT_LONG" || signal == "EXIT_SHORT" {
			// Only allow tick exit if it's a critical price break, otherwise defer to bar close
			if state.LatestPrice < (state.LiveSessionVWAP*0.995) || state.LatestPrice > (state.LiveSessionVWAP*1.005) {
				e.mu.Unlock()
				e.LogOptimizationExit(symbol, signal, state)
				return signal
			}
		}
	}

	e.mu.Unlock()
	return "HOLD"
}

// GenerateSignal is stripped of active trade logging hooks to prevent duplicate log events
func (e *Engine) GenerateSignal(symbol string, currentSide string, averagePrice float64, netQty int) string {
	e.mu.Lock()
	state, exists := e.Registry[symbol]
	if !exists || e.ActiveStrategy == nil {
		e.mu.Unlock()
		return "HOLD"
	}

	e.updateSignalPhaseAndExtensions(state, currentSide, averagePrice, netQty)
	e.mu.Unlock()

	isFlatNow := currentSide == "FLAT" || currentSide == ""
	if isFlatNow {
		return e.ActiveStrategy.CheckEntry(state)
	}
	return e.ActiveStrategy.CheckExit(state, currentSide)
}

func (e *Engine) LogOptimizationEntry(symbol string, signal string, state *InstrumentState) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if _, exists := e.ActiveTrades[symbol]; exists {
		return // Block entry duplication
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
		e.mu.Unlock()
		return
	}
	delete(e.ActiveTrades, symbol)
	state.CurrentSetupPhase = PhaseNeutral
	state.PeakEfficiency = 0.0
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

	// Compute Optimization Metric Capture Ratio immediately
	if tradeLog.PeakPnLINR > 0 {
		tradeLog.EfficiencyCaptureRatio = finalPnL / tradeLog.PeakPnLINR
	} else if tradeLog.PeakPnLINR == 0 && finalPnL == 0 {
		tradeLog.EfficiencyCaptureRatio = 1.0
	} else {
		tradeLog.EfficiencyCaptureRatio = -1.0 // Signifies negative drift from flat lines
	}

	go func(logRecord *OptimizationTradeLog) {
		pool := db.GetPool()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := db.LogStrategyOptimizationTradeExpanded(
			ctx, pool, logRecord.Symbol, logRecord.StrategyName, logRecord.TradeSide,
			logRecord.MinutesSinceOpen, logRecord.EntryTimestamp, logRecord.EntryPrice,
			logRecord.EntryVwap, logRecord.EntryVolumeRank, logRecord.EntryEfficiency,
			logRecord.EntryDelta, logRecord.EntrySlope, logRecord.EntryVwapDistance,
			logRecord.ExitTimestamp, logRecord.ExitPrice, logRecord.ExitReason,
			logRecord.FinalPnLINR, logRecord.PeakPnLINR, logRecord.EfficiencyCaptureRatio,
		)
		if err != nil {
			logger.Errorf("🚨 Optimization Engine direct write failed for %s: %v", logRecord.Symbol, err)
		}
	}(tradeLog)

	if e.OnTradeCompleted != nil {
		e.OnTradeCompleted(tradeLog)
	}
}
