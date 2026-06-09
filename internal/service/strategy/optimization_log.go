package strategy

import (
	"time"
)

// OptimizationTradeLog holds the freeze-frame microstructural variables
// required to analyze performance parameters cleanly.
type OptimizationTradeLog struct {
	Symbol           string
	StrategyName     string
	TradeSide        string // "LONG" or "SHORT"
	MinutesSinceOpen int

	// --- 📸 Entry Snapshot Attributes ---
	EntryTimestamp    time.Time
	EntryPrice        float64
	EntryVwap         float64
	EntryVolumeRank   int
	EntryPriceRank    int
	EntryWickRatio    float64
	EntryVwapDistance float64

	// --- 🎯 Outcome Attributes ---
	ExitTimestamp time.Time
	ExitPrice     float64
	ExitReason    string // "TAKE_PROFIT", "STOP_LOSS", "DIRECTION_FLIP"
	FinalPnLINR   float64
}
