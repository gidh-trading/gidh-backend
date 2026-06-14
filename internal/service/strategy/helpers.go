package strategy

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
)

const DecayConstant = 0.90

// ProcessClosedBarLedger decays individual registers and isolates high-impact energy parameters
func (e *Engine) ProcessClosedBarLedger(state *InstrumentState, bar *models.Bar) {
	// 1. Smoothly decay existing registers to maintain historical context memory
	state.Ledger.BullEfficient *= DecayConstant
	state.Ledger.BearEfficient *= DecayConstant
	state.Ledger.LastUpdated = bar.Timestamp

	// 2. Derive delta edge and full energy footprint using internal analytics ranks
	energy := float64(bar.Analytics.VolumeRank * bar.Analytics.PriceRank)
	delta := bar.Analytics.PriceRank - bar.Analytics.VolumeRank

	// 3. Classify and distribute values back into independent registers
	switch bar.Analytics.Direction {
	case models.DirStrongBullish, models.DirBullish:
		if math.Abs(float64(delta)) <= 1 {
			state.Ledger.BullEfficient += energy
		} else {
			state.Ledger.BullEfficient += energy * 0.5
		}

	case models.DirStrongBearish, models.DirBearish:
		if math.Abs(float64(delta)) <= 1 {
			state.Ledger.BearEfficient += energy
		} else {
			state.Ledger.BearEfficient += (energy * 0.5)
		}
	}
}

func (e *Engine) processActiveSignalRoute(symbol string, state *InstrumentState, side string, avgPrice float64, qty int) string {
	if e.ActiveStrategy != nil {
		return e.ActiveStrategy.CheckExit(state, side)
	}
	return "HOLD"
}

func (e *Engine) isBeforeMarketOpen(bar *models.Bar) bool {
	marketOpenTime := time.Date(bar.Timestamp.Year(), bar.Timestamp.Month(), bar.Timestamp.Day(), 9, 15, 0, 0, bar.Timestamp.Location())
	return bar.Timestamp.Before(marketOpenTime)
}

func (e *Engine) getOrInitializeState(symbol string) *InstrumentState {
	state, exists := e.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:               symbol,
			CurrentSetupPhase:    PhaseNeutral,
			BarHistory:           make(map[string][]*models.Bar),
			NetEfficiencyHistory: make([]float64, 0, 16), // Allocated cleanly for 10-bar slope lookback requirements
			NetEfficiency:        0.0,
			NetEfficiencySlope:   0.0,
			PeakEfficiency:       0.0,
		}
		if profile, ok := e.profiles[symbol]; ok {
			state.Profile = profile
		}
		e.Registry[symbol] = state
	}
	return state
}

func (e *Engine) updateCoreBarMetrics(state *InstrumentState, bar *models.Bar) {
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.LastUpdated = bar.Timestamp

	// 🛠️ FIX Bug #8 Context Support: Retain ranks from incoming closed bar contexts
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LatestPriceRank = bar.Analytics.PriceRank
}

func (e *Engine) updateCoreTickMetrics(state *InstrumentState, tick models.TickData) {
	state.LatestPrice = tick.LastPrice
	state.LastUpdated = tick.Timestamp

	// Ticks do not naturally possess bar analytics ranks; these remain cached from the last closed bar step
}

func (e *Engine) trackVwapAcceptance(state *InstrumentState, bar *models.Bar) {
	if bar.Close > bar.VWAP {
		state.ConsecutiveClosesAboveVwap++
		state.ConsecutiveClosesBelowVwap = 0
	} else if bar.Close < bar.VWAP {
		state.ConsecutiveClosesBelowVwap++
		state.ConsecutiveClosesAboveVwap = 0
	} else {
		state.ConsecutiveClosesAboveVwap = 0
		state.ConsecutiveClosesBelowVwap = 0
	}
}

// calculateNormalizedDistance maps structural deviations against ADRPct expected daily boundaries
func (e *Engine) calculateNormalizedDistance(price float64, vwap float64, profile *models.InstrumentProfile) float64 {
	if vwap == 0 || profile == nil || profile.ADRPct == 0 {
		return 0.0
	}
	expectedDailyVarianceRange := vwap * (profile.ADRPct / 100.0)
	if expectedDailyVarianceRange == 0 {
		return 0.0
	}
	return (price - vwap) / expectedDailyVarianceRange
}

func (e *Engine) appendAndPruneHistory(state *InstrumentState, bar *models.Bar) {
	timeframe := bar.Timeframe
	state.BarHistory[timeframe] = append(state.BarHistory[timeframe], bar)

	maxBars := int(e.MaxBarLookback / time.Minute)
	if maxBars <= 0 {
		maxBars = 100
	}
	if len(state.BarHistory[timeframe]) > maxBars {
		state.BarHistory[timeframe] = state.BarHistory[timeframe][1:]
	}
}

func (e *Engine) updateSignalPhaseAndExtensions(state *InstrumentState, currentSide string, averagePrice float64, netQty int) {
	if currentSide == "FLAT" || currentSide == "" || netQty == 0 {
		state.CurrentSetupPhase = PhaseNeutral
		state.EntryVwapAnchor = 0.0
		// Do not mutate or scrub peak indicators blindly inside the signal re-routing phase thread loops
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
		if state.EntryVwapAnchor == 0 {
			state.EntryVwapAnchor = state.LiveSessionVWAP
		}
	}
}

// CalculateLinearRegressionSlope returns the slope of historical metrics data points over time frames
func CalculateLinearRegressionSlope(values []float64) float64 {
	n := float64(len(values))
	if n < 2 {
		return 0.0 // Can't calculate a trendline on 1 or 0 points
	}

	var sumX, sumY, sumXY, sumXX float64
	for i, y := range values {
		x := float64(i)
		sumX += x
		sumY += y
		sumXY += x * y
		sumXX += x * x
	}

	denominator := (n * sumXX) - (sumX * sumX)
	if denominator == 0 {
		return 0.0
	}

	return (n*sumXY - sumX*sumY) / denominator
}
