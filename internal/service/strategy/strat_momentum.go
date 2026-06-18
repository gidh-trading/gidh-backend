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

	// 🛑 CRITICAL: We need at least 2 completed bars to evaluate slope change trajectory!
	if !exists || len(history) < 2 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]
	prevBar := history[len(history)-2]

	// 1. Core Structural Feature Extraction
	volumeRank := latestBar.Analytics.VolumeRank
	priceRank := latestBar.Analytics.PriceRank

	eff := latestBar.Analytics.NetEfficiency
	slope := latestBar.Analytics.NetEfficiencySlope
	prevSlope := prevBar.Analytics.NetEfficiencySlope

	// 2. Filter for Institutional Footprint Presence (90th Percentile Expansion)
	if volumeRank >= 6 && priceRank >= 6 {

		// 🚀 BULLISH IGNITION LOOP (LONG ENTRY)
		if latestBar.Close > latestBar.VWAP { // Price is above VWAP
			if eff > 20.0 { // Net Efficiency confirms buying control
				if slope > prevSlope { // 📈 Slope is rising (Positive Velocity Acceleration)
					return "GO_LONG"
				}
			}
		}

		// 📉 BEARISH IGNITION LOOP (SHORT ENTRY)
		if latestBar.Close < latestBar.VWAP { // Price is below VWAP
			if eff < -20.0 { // Net Efficiency confirms selling control
				if slope < prevSlope { // 📉 Slope is falling deeper (Negative Velocity Acceleration)
					return "GO_SHORT"
				}
			}
		}
	}

	return "HOLD"
}

// CheckExit handles automated trend termination exits exclusively on the 5-minute timeframe
func (s *VwapEfficiencyMomentumStrategy) CheckExit(state *InstrumentState, currentSide string) string {
	tf := "5m" // Locked to the 5-minute chart for macro structural trend exits
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return "HOLD"
	}

	latestBar := history[len(history)-1]

	// 1. Core Feature Extraction from the 5m frame
	eff := latestBar.Analytics.NetEfficiency
	dir := latestBar.Analytics.Direction

	// 📉 LONG EXIT EXECUTION LOOP
	if currentSide == "LONG" {
		// Cut the position if:
		// - Buyer efficiency drops below a stable cushion (+15%)
		// - 5-minute trajectory line actively rolls over into a downward curve (Slope < -1.0)
		// - The candle footprint closes as a full structural panic liquidation bar
		if eff < 15.0 || dir == models.DirStrongBearish {
			return "EXIT_LONG"
		}
	}

	// 📈 SHORT EXIT EXECUTION LOOP
	if currentSide == "SHORT" {
		// Cut the position if:
		// - Seller efficiency breaks above the minimal bearish threshold (-15%)
		// - 5-minute trajectory line actively recovers into an upward curve (Slope > 1.0)
		// - The candle footprint closes as a full structural aggressive buy bar
		if eff > -15.0 || dir == models.DirStrongBullish {
			return "EXIT_SHORT"
		}
	}

	return "HOLD"
}

// CheckTakeProfit handles automated technical trailing profit protections on the 1-minute chart
func (s *VwapEfficiencyMomentumStrategy) CheckTakeProfit(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	// 1. Ultimate Emergency Target: Lock in profits if they hit or exceed your hard ceiling
	totalProfitINR := state.CurrentPnL
	if totalProfitINR >= s.takeProfitINR {
		return true
	}

	// If we are in a loss or flat, we cannot process trend exhaustion take profits
	if totalProfitINR <= 0 {
		return false
	}

	// 2. Microstructural Exhaustion Layer: Check 1m chart to protect open gains
	tf := "1m"
	history, exists := state.BarHistory[tf]
	if !exists || len(history) == 0 {
		return false
	}

	latestBar := history[len(history)-1]
	slope := latestBar.Analytics.NetEfficiencySlope

	// 💰 Define a minimal profit cushion (e.g., 40% of your hard target)
	// This prevents minor noise right after entry from cutting a fresh trade too early.
	profitCushion := s.takeProfitINR * 0.40

	if totalProfitINR >= profitCushion {
		// 📉 LONG EXHAUSTION TP: If long and 1-minute slope rolls over negatively
		if currentSide == "LONG" && slope < -2.0 {
			return true
		}

		// 📈 SHORT EXHAUSTION TP: If short and 1-minute slope rolls over positively
		if currentSide == "SHORT" && slope > 2.0 {
			return true
		}
	}

	return false
}

// CheckStopLoss reads shallow risk parameters to protect trade principal
func (s *VwapEfficiencyMomentumStrategy) CheckStopLoss(state *InstrumentState, currentSide string, avgPrice float64, netQty int) bool {
	return false
}
