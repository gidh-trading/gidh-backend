package strategy

import "gidh-backend/internal/service/models"

type VwapEfficiencyMomentumStrategy struct {
	strategyName string

	// Scalping Config Thresholds
	effScalpThreshold     float64 // Minimum absolute efficiency for a single bar (e.g. 40.0)
	maxEffScalpThreshold  float64 // Maximum absolute efficiency to prevent chasing climax bars (e.g. 95.0)
	minVolumePriceRank    int     // Minimum institutional footprint rank (e.g. 5)
	longTimeAboveVwapPct  float64 // Minimum time spent above VWAP for long (e.g. 85.0)
	shortTimeAboveVwapPct float64 // Maximum time spent above VWAP for short (e.g. 15.0)

	// NEW: Simple Exit Thresholds
	exitEffThreshold float64 // Efficiency threshold to trigger an early exit (e.g., 30.0)
	takeProfitINR    float64 // Flat currency target for profit locking (e.g., 500.0 INR)
}

func NewVwapEfficiencyMomentumStrategy() *VwapEfficiencyMomentumStrategy {
	return &VwapEfficiencyMomentumStrategy{
		strategyName:          "High_Momentum_Scalp",
		effScalpThreshold:     40.0,
		maxEffScalpThreshold:  95.0,
		longTimeAboveVwapPct:  85.0,
		shortTimeAboveVwapPct: 15.0,
		exitEffThreshold:      50.0,
		takeProfitINR:         1000.0,
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
	timePctAboveVwap := latestBar.Analytics.TimePctAboveVwap
	eff := latestBar.Analytics.NetEfficiency
	dir := latestBar.Analytics.Direction // Cast to string for safety if it's a custom type

	if volumeRank == 6 && priceRank == 7 {
		// 🚀 LONG SCALP IGNITION
		if eff >= 35 && eff <= 95 {
			if timePctAboveVwap >= 75 {
				if dir == models.DirBullish || dir == models.DirStrongBullish {
					return "GO_LONG"
				}
			}
		}

		// 📉 SHORT SCALP IGNITION
		if eff <= -35 && eff >= -95 {
			if timePctAboveVwap <= 25 {
				if dir == models.DirBearish || dir == models.DirStrongBearish {
					return "GO_SHORT"
				}
			}
		}

	}

	return "HOLD"
}

// CheckExit shifts from a simple VWAP cross to cutting trends when speed completely dies
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "5m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	eff := latestBar.Analytics.NetEfficiency
	dir := latestBar.Analytics.Direction

	// 📉 LONG EXIT: Momentum reverses heavily downward
	if currentSide == "LONG" {
		if eff < 10 || dir == models.DirStrongBearish {
			return "EXIT_LONG"
		}
	}

	// 📈 SHORT EXIT: Momentum reverses heavily upward
	if currentSide == "SHORT" {
		if eff > -10 || dir == models.DirStrongBullish {
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
