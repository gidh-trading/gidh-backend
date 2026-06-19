package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
)

const (
	InitialTakeProfitINR = 500.0 // Starting take profit target ceiling
	DecayRatePerMinute   = 10.0  // Linear decay modifier per minute passed
	MinTakeProfitFloor   = 150.0 // The absolute minimum profit target allowed after decay
)

type VwapEfficiencyMomentumStrategy struct {
	strategyName string
	mu           sync.RWMutex
	configs      map[string]*models.OptimizedStrategyConfig // Kept for compatibility but bypassed
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

// CheckEntry executes our consecutive-candle confirmation strictly on the 1m timeframe
func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	tf := "1m" // Shifting execution layer entirely to the 1-minute chart for pure timing precision
	history, exists := state.BarHistory[tf]

	// 🛑 CRITICAL: We need at least 2 completed bars to evaluate consecutive candle rules!
	if !exists || len(history) < 2 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	prevBar := history[len(history)-2]

	// Extract features from both 1-minute bars to confirm a true momentum impulse
	latestVolumeRank := latestBar.Analytics.VolumeRank
	latestPriceRank := latestBar.Analytics.PriceRank
	latestDir := latestBar.Analytics.Direction

	prevVolumeRank := prevBar.Analytics.VolumeRank
	prevPriceRank := prevBar.Analytics.PriceRank
	prevDir := prevBar.Analytics.Direction

	// 🚀 BULLISH HUMAN IGNITION LOOP (LONG ENTRY)
	// - Both 1m candles: Price Rank >= 75th percentile (Rank 5+)
	// - Both 1m candles: Volume Rank >= 75th percentile (Rank 5+)
	// - Both 1m candles: Tape direction is verified as BULLISH or STRONG_BULLISH
	// - Both 1m candles: Closed cleanly above their respective live session VWAP benchmarks
	// - Both 1m candles: Closed as positive green expansion bars (Close > Open)
	if latestPriceRank >= 5 && latestVolumeRank >= 5 && prevPriceRank >= 5 && prevVolumeRank >= 5 {
		isLatestBullishDir := latestDir == models.DirBullish || latestDir == models.DirStrongBullish
		isPrevBullishDir := prevDir == models.DirBullish || prevDir == models.DirStrongBullish

		if isLatestBullishDir && isPrevBullishDir {
			if latestBar.Close > latestBar.VWAP && prevBar.Close > prevBar.VWAP {
				if latestBar.Close > latestBar.Open && prevBar.Close > prevBar.Open {
					return "GO_LONG"
				}
			}
		}
	}

	// 📉 BEARISH HUMAN IGNITION LOOP (SHORT ENTRY)
	// - Both 1m candles: Price Rank <= 25th percentile (Rank 3 or lower)
	// - Both 1m candles: Volume Rank >= 75th percentile (Rank 5+)
	// - Both 1m candles: Tape direction is verified as BEARISH or STRONG_BEARISH
	// - Both 1m candles: Closed cleanly below their respective live session VWAP benchmarks
	// - Both 1m candles: Closed as negative red liquidation bars (Close < Open)
	if latestPriceRank <= 3 && latestVolumeRank >= 5 && prevPriceRank <= 3 && prevVolumeRank >= 5 {
		isLatestBearishDir := latestDir == models.DirBearish || latestDir == models.DirStrongBearish
		isPrevBearishDir := prevDir == models.DirBearish || prevDir == models.DirStrongBearish

		if isLatestBearishDir && isPrevBearishDir {
			if latestBar.Close < latestBar.VWAP && prevBar.Close < prevBar.VWAP {
				if latestBar.Close < latestBar.Open && prevBar.Close < prevBar.Open {
					return "GO_SHORT"
				}
			}
		}
	}

	return "HOLD"
}

// CheckExit trend breakdown rules are deactivated for pure entry/flat target isolation testing
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	return "HOLD"
}

// CheckTakeProfit handles our dynamic Time-Decaying Profit Target matching the rapid 1m entry profile
func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	if state.EntryTimestamp.IsZero() {
		return state.CurrentPnL >= InitialTakeProfitINR
	}

	// 1. Calculate the active duration the trade has been alive
	marketTime := state.LastTickTime
	durationAlive := marketTime.Sub(state.EntryTimestamp)
	minutesElapsed := durationAlive.Minutes()

	// 2. Compute linearly decayed target amount
	decayAmount := minutesElapsed * DecayRatePerMinute
	dynamicTargetProfit := InitialTakeProfitINR - decayAmount

	// 3. Enforce the baseline target floor protection
	if dynamicTargetProfit < MinTakeProfitFloor {
		dynamicTargetProfit = MinTakeProfitFloor
	}

	// 4. Fire exit if currency PnL hits the decayed threshold target
	if state.CurrentPnL >= dynamicTargetProfit {
		return true
	}

	return false
}

// CheckStopLoss rules are completely deactivated for this sandbox test run
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return false
}
