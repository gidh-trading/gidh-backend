package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
)

// PositionRisk handles the live tracking state, protection boundaries, and milestone scaling
type PositionRisk struct {
	IsActive       bool
	Side           string // "LONG" or "SHORT"
	EntryPrice     float64
	TargetP75      float64
	TargetP90      float64
	CurrentSL      float64
	P75SliceTaken  bool
	ActiveInterval string // Monitored interval, e.g., "1m"
}

type ScalperAgent struct {
	mu              sync.RWMutex
	pricePotentials models.TargetMatrix                // Injected map: stock_name -> timeframe -> PricePotential
	activePositions map[string]*PositionRisk           // Map of live positions tracking risk boundaries per stock name
	stateWindows    map[string][]models.DirectionState // Memory tracking of consecutive sliding states
	windowSize      int                                // Length of state lookback buffer (e.g., 5 rolling updates)
}

// NewScalperAgent instantiates the atomic execution machine with its statistical metrics mapped fully on creation
func NewScalperAgent(stateWindowSize int, staticMatrix models.TargetMatrix) *ScalperAgent {
	return &ScalperAgent{
		pricePotentials: staticMatrix,
		activePositions: make(map[string]*PositionRisk),
		stateWindows:    make(map[string][]models.DirectionState),
		windowSize:      stateWindowSize,
	}
}

// ========================================================================
// 🏛️ PIPELINE STREAM ENTRY INTERCEPTOR
// ========================================================================

// AnalyzeMarket evaluates the continuous tick feed against institutional indicators and trailing risk states
func (sa *ScalperAgent) AnalyzeMarket(tick *models.EnrichedTick) (string, bool) {
	sa.mu.Lock()
	symbol := tick.Raw.StockName
	direction := tick.Enrichment.Direction

	// 1. Ingest the current live state into the moving lookback window buffer
	sa.stateWindows[symbol] = append(sa.stateWindows[symbol], direction)
	if len(sa.stateWindows[symbol]) > sa.windowSize {
		sa.stateWindows[symbol] = sa.stateWindows[symbol][1:]
	}

	// 2. Safely initialize target position structure maps if unallocated
	if sa.activePositions[symbol] == nil {
		sa.activePositions[symbol] = &PositionRisk{IsActive: false}
	}
	pos := sa.activePositions[symbol]
	sa.mu.Unlock()

	// 3. Routing Layer: If currently holding risk, run defensive exit scans; else, hunt entries
	if pos.IsActive {
		return sa.evaluateStateBasedExits(tick, pos)
	}

	return sa.evaluateInstitutionalEntries(tick, pos)
}

// ========================================================================
// 🛠️ INSTANT ENTRY ENGINE (TICK SPEED)
// ========================================================================

func (sa *ScalperAgent) evaluateInstitutionalEntries(tick *models.EnrichedTick, pos *PositionRisk) (string, bool) {
	// Look for extreme volume urgency matching sharp rolling price velocity
	isInstitutionalVolume := tick.Enrichment.VolumeRank >= 6
	isImpactingPrice := tick.Enrichment.PriceRank >= 6

	if !isInstitutionalVolume || !isImpactingPrice {
		return "", false
	}

	symbol := tick.Raw.StockName
	direction := tick.Enrichment.Direction

	// Verify our static target memory matrix contains valid profiles before setting trades
	sa.mu.RLock()
	stockMatrix, hasStock := sa.pricePotentials[symbol]
	sa.mu.RUnlock()
	if !hasStock {
		return "", false
	}

	// Use "1m" as our initial protective validation anchor row
	stats, hasInterval := stockMatrix["1m"]
	if !hasInterval {
		return "", false
	}

	// TRIGGER LONG EXECUTION
	if direction == models.DirStrongBullish || direction == models.DirBullish {
		pos.IsActive = true
		pos.Side = "LONG"
		pos.EntryPrice = tick.Raw.LastPrice
		pos.TargetP75 = stats.P75
		pos.TargetP90 = stats.P90
		// Defensive Initial Stop Loss: Set tightly to the entry minus a small slice of 1m volatility
		pos.CurrentSL = tick.Raw.LastPrice - (stats.P75 * 0.25)
		pos.P75SliceTaken = false
		pos.ActiveInterval = "1m"

		return "GO_LONG", true
	}

	// TRIGGER SHORT EXECUTION
	if direction == models.DirStrongBearish || direction == models.DirBearish {
		pos.IsActive = true
		pos.Side = "SHORT"
		pos.EntryPrice = tick.Raw.LastPrice
		pos.TargetP75 = stats.P75
		pos.TargetP90 = stats.P90
		pos.CurrentSL = tick.Raw.LastPrice + (stats.P75 * 0.25)
		pos.P75SliceTaken = false
		pos.ActiveInterval = "1m"

		return "GO_SHORT", true
	}

	return "", false
}

// ========================================================================
// 🛡️ RISK MONITOR & PULLBACK FILTER
// ========================================================================

func (sa *ScalperAgent) evaluateStateBasedExits(tick *models.EnrichedTick, pos *PositionRisk) (string, bool) {
	currentPrice := tick.Raw.LastPrice
	symbol := tick.Raw.StockName

	// ========================================================================
	// 🟢 LONG DEFENSIVE EXECUTION
	// ========================================================================
	if pos.Side == "LONG" {
		// Rule A: Immediate Safety SL Triggered (Hard stop or trailing target protection floor)
		if currentPrice <= pos.CurrentSL {
			pos.IsActive = false
			return "LIQUIDATE_ALL_LONG", true
		}

		// Rule B: Milestone 1 (p75 Met) -> Bank 50% Profit immediately and drag SL to Breakeven
		if currentPrice >= (pos.EntryPrice+pos.TargetP75) && !pos.P75SliceTaken {
			pos.P75SliceTaken = true
			pos.CurrentSL = pos.EntryPrice // Risk eliminated entirely
			return "SLICE_50_PERCENT_LONG", true
		}

		// Rule C: Milestone 2 (p90 Met) -> Move trailing SL up to secure Milestone 1 profit level
		p75PriceLevel := pos.EntryPrice + pos.TargetP75
		if currentPrice >= (pos.EntryPrice+pos.TargetP90) && pos.CurrentSL < p75PriceLevel {
			pos.CurrentSL = p75PriceLevel
		}

		// Rule D: High-Volume Reversal Override (Immediate Institutional Counter-Attack)
		if tick.Enrichment.Direction == models.DirStrongBearish && tick.Enrichment.VolumeRank >= 6 {
			pos.IsActive = false
			return "LIQUIDATE_ALL_LONG", true
		}

		// Rule E: Continuous State Window Decay (Filters out isolated bar noise)
		sa.mu.RLock()
		window := sa.stateWindows[symbol]
		sa.mu.RUnlock()

		bullishEnergyCount := 0
		for _, state := range window {
			if state == models.DirStrongBullish || state == models.DirBullish || state == models.DirBullishAbsorption {
				bullishEnergyCount++
			}
		}
		// If less than 20% of our recent rolling history contains bullish activity, the momentum has died
		if len(window) == sa.windowSize && float64(bullishEnergyCount)/float64(sa.windowSize) < 0.20 {
			pos.IsActive = false
			return "LIQUIDATE_ALL_LONG", true
		}
	}

	// ========================================================================
	// 🔴 SHORT DEFENSIVE EXECUTION
	// ========================================================================
	if pos.Side == "SHORT" {
		// Rule A: Immediate Safety SL Triggered
		if currentPrice >= pos.CurrentSL {
			pos.IsActive = false
			return "LIQUIDATE_ALL_SHORT", true
		}

		// Rule B: Milestone 1 (p75 Met) -> Bank 50% Profit and drag SL to Breakeven
		if currentPrice <= (pos.EntryPrice-pos.TargetP75) && !pos.P75SliceTaken {
			pos.P75SliceTaken = true
			pos.CurrentSL = pos.EntryPrice
			return "SLICE_50_PERCENT_SHORT", true
		}

		// Rule C: Milestone 2 (p90 Met) -> Move trailing SL down to secure Milestone 1 short level
		p75PriceLevel := pos.EntryPrice - pos.TargetP75
		if currentPrice <= (pos.EntryPrice-pos.TargetP90) && pos.CurrentSL > p75PriceLevel {
			pos.CurrentSL = p75PriceLevel
		}

		// Rule D: High-Volume Reversal Override
		if tick.Enrichment.Direction == models.DirStrongBullish && tick.Enrichment.VolumeRank >= 6 {
			pos.IsActive = false
			return "LIQUIDATE_ALL_SHORT", true
		}

		// Rule E: Continuous State Window Decay
		sa.mu.RLock()
		window := sa.stateWindows[symbol]
		sa.mu.RUnlock()

		bearishEnergyCount := 0
		for _, state := range window {
			if state == models.DirStrongBearish || state == models.DirBearish || state == models.DirBearishAbsorption {
				bearishEnergyCount++
			}
		}
		if len(window) == sa.windowSize && float64(bearishEnergyCount)/float64(sa.windowSize) < 0.20 {
			pos.IsActive = false
			return "LIQUIDATE_ALL_SHORT", true
		}
	}

	return "", false
}

// UpgradeActiveTargets transitions an open position's targets to wider timeframes (Expansion Unlock)
func (sa *ScalperAgent) UpgradeActiveTargets(stock string, targetTimeframe string) bool {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	pos, hasPos := sa.activePositions[stock]
	if !hasPos || !pos.IsActive {
		return false
	}

	stockMatrix, hasStock := sa.pricePotentials[stock]
	if !hasStock {
		return false
	}

	stats, hasInterval := stockMatrix[targetTimeframe]
	if !hasInterval {
		return false
	}

	pos.TargetP75 = stats.P75
	pos.TargetP90 = stats.P90
	pos.ActiveInterval = targetTimeframe

	return true
}

// ResetPositionState allows the outside execution coordinator to clear out tracking markers manually
func (sa *ScalperAgent) ResetPositionState(stock string) {
	sa.mu.Lock()
	defer sa.mu.Unlock()
	if sa.activePositions[stock] != nil {
		sa.activePositions[stock].IsActive = false
	}
}
