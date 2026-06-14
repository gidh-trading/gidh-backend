package strategy

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
)

// ProcessClosedBarLedger applies structural calculations and updates the persistent indicators
func (e *Engine) ProcessClosedBarLedger(state *InstrumentState, bar *models.Bar) {
	// 1. If your upstream pipeline passes pre-calculated analytics, protect it here.
	// Otherwise, calculate the raw net efficiency for this single bar context.
	if bar.Analytics.NetEfficiency == 0 {
		// Fallback baseline calculation using price vs volume metrics
		energy := float64(bar.Analytics.VolumeRank * bar.Analytics.PriceRank)
		delta := bar.Analytics.PriceRank - bar.Analytics.VolumeRank

		// Establish structural sign allocation base
		sign := 0.0
		if bar.Analytics.Direction == models.DirStrongBullish || bar.Analytics.Direction == models.DirBullish {
			sign = 1.0
		} else if bar.Analytics.Direction == models.DirStrongBearish || bar.Analytics.Direction == models.DirBearish {
			sign = -1.0
		}

		// Close tracking alignment parameters
		if math.Abs(float64(delta)) <= 1 {
			bar.Analytics.NetEfficiency = energy * sign
		} else {
			bar.Analytics.NetEfficiency = (energy * 0.5) * sign // Reduce impact of vacuum/chase bars
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
			NetEfficiencyHistory: make([]float64, 0, 10), // Lightweight rolling sequence buffer allocation
			NetEfficiency:        0.0,
			NetEfficiencySlope:   0.0,
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
}

func (e *Engine) updateCoreTickMetrics(state *InstrumentState, tick models.TickData) {
	state.LatestPrice = tick.LastPrice
	state.LastUpdated = tick.Timestamp
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

// calculateNormalizedDistance maps structural deviations against ADRPct expected boundary frameworks
func (e *Engine) calculateNormalizedDistance(price float64, vwap float64, profile *models.InstrumentProfile) float64 {
	if vwap == 0 || profile == nil || profile.ADRPct == 0 {
		return 0.0
	}

	// Convert asset daily percentage expectation to localized pricing currency values
	// e.g., standard baseline expected currency variance threshold
	expectedDailyVarianceRange := vwap * (profile.ADRPct / 100.0)
	if expectedDailyVarianceRange == 0 {
		return 0.0
	}

	// Returns normalized contextual variance band ratios relative to basic index anchors
	return (price - vwap) / expectedDailyVarianceRange
}

func (e *Engine) appendAndPruneHistory(state *InstrumentState, bar *models.Bar) {
	timeframe := bar.Timeframe
	state.BarHistory[timeframe] = append(state.BarHistory[timeframe], bar)

	// Trim trailing tail blocks to strictly avoid memory leak accumulation profiles
	maxBars := int(e.MaxBarLookback / time.Minute)
	if maxBars <= 0 {
		maxBars = 100 // Safe operational fall-back
	}
	if len(state.BarHistory[timeframe]) > maxBars {
		state.BarHistory[timeframe] = state.BarHistory[timeframe][1:]
	}
}

func (e *Engine) updateSignalPhaseAndExtensions(state *InstrumentState, currentSide string, averagePrice float64, netQty int) {
	if currentSide == "FLAT" || currentSide == "" || netQty == 0 {
		state.CurrentSetupPhase = PhaseNeutral
		state.EntryVwapAnchor = 0.0
		state.PeakVwapExtension = 0.0
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
		if state.EntryVwapAnchor == 0 {
			state.EntryVwapAnchor = state.LiveSessionVWAP
		}

		dist := math.Abs(state.NormalizedVwapDistance)
		if dist > state.PeakVwapExtension {
			state.PeakVwapExtension = dist
		}
	}
}
