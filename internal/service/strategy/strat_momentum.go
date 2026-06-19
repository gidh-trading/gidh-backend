package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	InitialTakeProfitINR = 1000.0 // Starting take profit target ceiling
	DecayRatePerMinute   = 10.0   // Linear decay modifier per minute passed
	MinTakeProfitFloor   = 150.0  // The absolute minimum profit target allowed after decay
	HardStopLossINR      = -400.0 // Strict monetary guillotine (1:2 base Risk/Reward)
)

type VwapEfficiencyMomentumStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig
}

func NewVwapEfficiencyMomentumStrategy(configs map[string]*models.OptimizedStrategyConfig) *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName: "Human_1m_Momentum_Scalp",
		configs:      configs,
	}
}

func (s *VwapEfficiencyMomentumStrategy) Name() string {
	return s.strategyName
}

func (s *VwapEfficiencyMomentumStrategy) UpdateConfigs(newConfigs map[string]*models.OptimizedStrategyConfig) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configs = newConfigs
}

// CheckEntry executes our confirmation strictly on the closed 1m timeframe
func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]

	// 🛑 We only need 1 completed bar for a true Ignition entry
	if !exists || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]

	// Extract features of the immediate closed candle
	latestVolRank := latestBar.Analytics.VolumeRank
	latestPriceRank := latestBar.Analytics.PriceRank
	latestDir := latestBar.Analytics.Direction
	latestSlope := latestBar.Analytics.NetEfficiencySlope

	// 🔥 1-CANDLE IGNITION: We only care about the absolute latest closed footprint.
	// If an institution steps in right now with massive volume and price expansion, we follow immediately.
	isIgnition := latestVolRank >= 6 && latestPriceRank >= 6

	if !isIgnition {
		return "HOLD"
	}

	// 🚀 BULLISH IGNITION (LONG ENTRY)
	isLatestBullishDir := latestDir == models.DirBullish || latestDir == models.DirStrongBullish

	if isLatestBullishDir {
		if latestBar.Close > latestBar.VWAP {
			if latestSlope > 10.0 { // Underlying order flow trajectory must be aggressively positive
				return "GO_LONG"
			}
		}
	}

	// 📉 BEARISH IGNITION (SHORT ENTRY)
	isLatestBearishDir := latestDir == models.DirBearish || latestDir == models.DirStrongBearish

	if isLatestBearishDir {
		if latestBar.Close < latestBar.VWAP {
			if latestSlope < -10.0 { // Underlying order flow trajectory must be aggressively negative
				return "GO_SHORT"
			}
		}
	}

	return "HOLD"
}

// CheckExit trend breakdown rules are deactivated for pure momentum tracking
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}

// CheckTakeProfit handles our dynamic Time-Decaying Profit Target
func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfitINR
	}

	// 1. Calculate active trade duration
	marketTime := state.LastTickTime
	durationAlive := marketTime.Sub(state.EntryTimestamp)
	minutesElapsed := durationAlive.Minutes()

	// 2. Compute linearly decayed target amount
	decayAmount := minutesElapsed * DecayRatePerMinute
	dynamicTargetProfit := InitialTakeProfitINR - decayAmount

	// 3. Enforce target floor protection
	if dynamicTargetProfit < MinTakeProfitFloor {
		dynamicTargetProfit = MinTakeProfitFloor
	}

	// 4. Fire exit if PnL hits the dynamic threshold
	if state.CurrentPnL >= dynamicTargetProfit {
		return true
	}

	return false
}

// CheckStopLoss enforces a strict "it goes right away or we are out" philosophy
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// 1. THE MONETARY GUILLOTINE
	if state.CurrentPnL <= HardStopLossINR {
		return true
	}

	// 2. THE MOMENTUM INVALIDATION (Structural Failure)
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if exists && len(history) >= 1 && !state.EntryTimestamp.IsZero() {

		// Find the specific candle that triggered the entry
		var entryBar *models.Bar
		for i := len(history) - 1; i >= 0; i-- {
			// Match the candle timestamp to the minute we entered
			if history[i].Timestamp.Minute() == state.EntryTimestamp.Minute() {
				entryBar = history[i]
				break
			}
		}

		// If we found the original entry candle, enforce the line in the sand
		if entryBar != nil {
			if currentSide == "LONG" {
				if state.LatestPrice < entryBar.Low {
					return true
				}
			}
			if currentSide == "SHORT" {
				if state.LatestPrice > entryBar.High {
					return true
				}
			}
		}
	}

	return false
}
