package strategy

import (
	"gidh-backend/internal/service/models"
)

type StructuralReversionStrategy struct {
	cfg *Config
}

func NewStructuralReversionStrategy() *StructuralReversionStrategy {
	return &StructuralReversionStrategy{
		cfg: &Config{
			StartTradingTime:   915,  // 09:15 AM
			EndTradingTime:     1455, // 02:55 PM
			ForceExitTime:      1500, // 03:00 PM Auto Square-Off
			HardStopLossINR:    -1000.0,
			TakeProfitINR:      1500.0,
			MaximumTradesCount: 1,
		},
	}
}

func (s *StructuralReversionStrategy) Name() string {
	return "Structural_Value_Reversion"
}

func (s *StructuralReversionStrategy) Config() *Config {
	return s.cfg
}

// CheckEntry monitors structural confluence across ADR bounds and flat Volume Profile layers
func (s *StructuralReversionStrategy) CheckEntry(state *InstrumentState) string {
	// 1. Fetch the historical bar slice (we need at least 5 bars for the lookback check)
	history, ok := state.BarHistory["1m"]
	if !ok || len(history) < 5 {
		return "HOLD"
	}

	// 2. Extract the target execution bar (the most recent completed bar)
	latestBar := history[len(history)-1]
	if latestBar == nil {
		return "HOLD"
	}

	currentPrice := latestBar.Close
	adrHigh := latestBar.Analytics.ADRHigh
	adrLow := latestBar.Analytics.ADRLow
	targetVAH := latestBar.VAH
	targetVAL := latestBar.VAL

	// Safety check: Ensure structural profile lines have actively initialized
	if adrHigh == 0 || adrLow == 0 || targetVAH == 0 || targetVAL == 0 {
		return "HOLD"
	}

	// 3. LOOKBACK VALIDATION: Verify the Value Area lines have been perfectly flat for the last 5 bars
	isVahFlat := true
	isValFlat := true

	// Loop backward from the end of the history slice to check the last 5 bars
	for i := len(history) - 1; i >= len(history)-5; i-- {
		if history[i] == nil {
			return "HOLD"
		}
		if history[i].VAH != targetVAH {
			isVahFlat = false
		}
		if history[i].VAL != targetVAL {
			isValFlat = false
		}
	}

	// 4. SHORT CONDITION: Extended past ADR High, VAH is below ADR High, and VAH has held perfectly flat
	if currentPrice > adrHigh && targetVAH < adrHigh && isVahFlat {
		return "GO_SHORT"
	}

	// 5. LONG CONDITION: Dropped below ADR Low, VAL is above ADR Low, and VAL has held perfectly flat
	if currentPrice < adrLow && targetVAL > adrLow && isValFlat {
		return "GO_LONG"
	}

	return "HOLD"
}

func (s *StructuralReversionStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD" // Handled globally via target TakeProfit/StopLoss parameters
}

func (s *StructuralReversionStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, averagePrice float64, netQty int, percentiles map[string]*models.VWAPDistancePercentile) bool {
	return state.CurrentPnL >= s.cfg.TakeProfitINR
}

func (s *StructuralReversionStrategy) CheckStopLoss(state *InstrumentState, currentSide string, averagePrice float64, netQty int, profiles map[string]*models.InstrumentProfile) bool {
	return state.CurrentPnL <= s.cfg.HardStopLossINR
}

func (s *StructuralReversionStrategy) OnEntryCommit(state *InstrumentState, symbol string) {
	state.ActiveStrategyName = s.Name()
	if state.StrategyHistory == nil {
		state.StrategyHistory = make(map[string]StrategyStats)
	}

	stats := state.StrategyHistory[s.Name()]
	stats.IsCurrentlyActive = true
	state.StrategyHistory[s.Name()] = stats
}
