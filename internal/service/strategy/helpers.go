package strategy

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
)

// ProcessClosedBarLedger applies structural calculations and updates the persistent decayed registers.
func (e *Engine) ProcessClosedBarLedger(state *InstrumentState, bar *models.Bar) {
	// 1. Decay existing registers smoothly (No dropout cliffs)
	state.Ledger.BullEfficient *= DecayConstant
	state.Ledger.BearEfficient *= DecayConstant
	state.Ledger.BullAbsorption *= DecayConstant
	state.Ledger.BearAbsorption *= DecayConstant
	state.Ledger.BullVacuum *= DecayConstant
	state.Ledger.BearVacuum *= DecayConstant

	// 2. Derive delta edge and full energy footprint using internal analytics ranks
	energy := float64(bar.Analytics.VolumeRank * bar.Analytics.PriceRank)
	delta := bar.Analytics.PriceRank - bar.Analytics.VolumeRank

	var currentState LedgerState = StateUndetermined

	// 3. Classify and allocate raw energy values based on Auction Framework directions
	switch bar.Analytics.Direction {
	case models.DirStrongBullish, models.DirBullish:
		if math.Abs(float64(delta)) <= 1 {
			currentState = StateEfficientBull
			state.Ledger.BullEfficient += energy
		} else if delta > 1 {
			currentState = StateBullVacuum
			state.Ledger.BullVacuum += energy
		}

	case models.DirStrongBearish, models.DirBearish:
		if math.Abs(float64(delta)) <= 1 {
			currentState = StateEfficientBear
			state.Ledger.BearEfficient += energy
		} else if delta > 1 {
			currentState = StateBearVacuum
			state.Ledger.BearVacuum += energy
		}

	case models.DirBullishAbsorption:
		currentState = StateBullAbsorption
		state.Ledger.BullAbsorption += energy

	case models.DirBearishAbsorption:
		currentState = StateBearAbsorption
		state.Ledger.BearAbsorption += energy
	}

	state.Ledger.LastUpdated = bar.Timestamp

	// 4. Update the MicroContext Tactical Trigger Array
	state.TriggerContext.RecentStates = append(state.TriggerContext.RecentStates, currentState)

	// Clamp rolling slice length to the constant TriggerLookback parameters
	if len(state.TriggerContext.RecentStates) > TriggerLookback {
		state.TriggerContext.RecentStates = state.TriggerContext.RecentStates[1:]
	}
}

func (e *Engine) updateCoreBarMetrics(state *InstrumentState, bar *models.Bar) {
	state.LatestPrice = bar.Close
	state.LiveSessionVWAP = bar.VWAP
	state.LatestPriceRank = bar.Analytics.PriceRank
	state.LatestVolumeRank = bar.Analytics.VolumeRank
	state.LastUpdated = bar.Timestamp

	if bar.Analytics.VolumeRank > 0 {
		sign := 1.0
		if bar.Close < bar.Open {
			sign = -1.0
		}
		state.Efficiency = (float64(bar.Analytics.PriceRank) / float64(bar.Analytics.VolumeRank)) * sign
	}
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

func (e *Engine) calculateNormalizedDistance(price, vwap float64, profile *models.InstrumentProfile) float64 {
	if vwap == 0 {
		return 0
	}
	return (price - vwap) / vwap
}

func (e *Engine) appendAndPruneHistory(state *InstrumentState, bar *models.Bar) {
	state.BarHistory[state.Symbol] = append(state.BarHistory[state.Symbol], bar)
	if len(state.BarHistory[state.Symbol]) > 100 {
		state.BarHistory[state.Symbol] = state.BarHistory[state.Symbol][1:]
	}
}

func (e *Engine) updateCoreTickMetrics(state *InstrumentState, tick models.TickData) {
	state.LatestPrice = tick.LastPrice
	state.LastUpdated = tick.Timestamp
}

func (e *Engine) evaluateFlatTickEntry(state *InstrumentState, adrMult float64) string {
	return "HOLD"
}

func (e *Engine) evaluateActiveTickPosition(state *InstrumentState, symbol, side string, avgPrice float64, qty int, adrMult float64) string {
	return "HOLD"
}

func (e *Engine) updateSignalPhaseAndExtensions(state *InstrumentState, side string, avgPrice float64, qty int) {
	if qty == 0 {
		state.CurrentSetupPhase = PhaseNeutral
	} else {
		state.CurrentSetupPhase = PhaseActiveTrade
	}
}

func (e *Engine) processNeutralSignalRoute(symbol string, state *InstrumentState) string {
	if e.ActiveStrategy != nil {
		return e.ActiveStrategy.CheckEntry(state)
	}
	return "HOLD"
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
			Symbol:            symbol,
			CurrentSetupPhase: PhaseNeutral,
			BarHistory:        make(map[string][]*models.Bar),
			TriggerContext: MicroContext{
				RecentStates: make([]LedgerState, 0, TriggerLookback),
			},
		}
		if profile, ok := e.profiles[symbol]; ok {
			state.Profile = profile
		}
		e.Registry[symbol] = state
	}
	return state
}
