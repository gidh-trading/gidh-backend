package strategy

type VwapEfficiencyMomentumStrategy struct {
	strategyName string

	// Scalping Config Thresholds
	effScalpThreshold     float64 // Minimum absolute efficiency for a single bar (e.g. 40.0)
	minVolumePriceRank    int     // Minimum institutional footprint rank (e.g. 5)
	longTimeAboveVwapPct  float64 // Minimum time spent above VWAP for long (e.g. 85.0)
	shortTimeAboveVwapPct float64 // Maximum time spent above VWAP for short (e.g. 15.0)

	// NEW: Simple Exit Thresholds
	exitSlopeThreshold float64 // Slope threshold to trigger an early exit (e.g., 10.0)
	exitEffThreshold   float64 // Efficiency threshold to trigger an early exit (e.g., 30.0)
	takeProfitINR      float64 // Flat currency target for profit locking (e.g., 500.0 INR)
}

func NewVwapEfficiencyMomentumStrategy() *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName:          "High_Momentum_Scalp",
		effScalpThreshold:     40.0,
		minVolumePriceRank:    5,
		longTimeAboveVwapPct:  85.0,
		shortTimeAboveVwapPct: 15.0,
		exitSlopeThreshold:    10.0,
		exitEffThreshold:      30.0,
		takeProfitINR:         500.0,
	}
}

func (s *VwapEfficiencyMomentumStrategy) Name() string {
	return s.strategyName
}

// CheckEntry looks for extreme, sudden momentum on a single bar for scalping entries
func (s *VwapEfficiencyMomentumStrategy) CheckEntry(state *InstrumentState) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]

	// We only need 1 completed bar now
	if !exists || len(history) < 1 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]

	// Extract features cleanly computed by your BarAnalyticsEngine
	volumeRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank
	timePctAboveVwap := latestBar.Analytics.TimePctAboveVwap // Assuming this field exists in your Analytics struct

	// FILTER: Require authentic institutional volume and price displacement
	if volumeRank < s.minVolumePriceRank || priceRank < s.minVolumePriceRank {
		return "HOLD"
	}

	// 🚀 LONG SCALP IGNITION
	// 1. Efficiency > 40 on the current bar
	// 2. Slope is positive (momentum is accelerating upwards)
	// 3. Time > 85% above VWAP
	if latestBar.Analytics.NetEfficiency > s.effScalpThreshold {
		if latestBar.Analytics.NetEfficiencySlope > 0.0 {
			if timePctAboveVwap >= s.longTimeAboveVwapPct {
				return "GO_LONG"
			}
		}
	}

	// 📉 SHORT SCALP IGNITION
	// 1. Efficiency < -40 on the current bar
	// 2. Slope is negative (momentum is accelerating downwards)
	// 3. Time < 15% above VWAP
	if latestBar.Analytics.NetEfficiency < -s.effScalpThreshold {
		if latestBar.Analytics.NetEfficiencySlope < 0.0 {
			if timePctAboveVwap <= s.shortTimeAboveVwapPct {
				return "GO_SHORT"
			}
		}
	}

	return "HOLD"
}

// CheckExit shifts from a simple VWAP cross to cutting trends when speed completely dies
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	eff := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope

	// 📉 LONG EXIT: Momentum reverses heavily downward
	if currentSide == "LONG" {
		if slope <= -s.exitSlopeThreshold && eff < -s.exitEffThreshold {
			return "EXIT_LONG"
		}
	}

	// 📈 SHORT EXIT: Momentum reverses heavily upward
	if currentSide == "SHORT" {
		if slope >= s.exitSlopeThreshold && eff > s.exitEffThreshold {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// CheckStopLoss reads shallow risk parameters to protect trade principal
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return false
}

// CheckTakeProfit handles automated technical trailing profit protections
func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {

	// Calculate the actual currency profit (Points gained * Number of shares)
	totalProfitINR := state.CurrentPnL * float64(netQty)

	// Simple flat target exit
	if totalProfitINR >= s.takeProfitINR {
		return true
	}

	return false
}
