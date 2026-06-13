package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

// SetupPhase defines the state machine flags for active execution tracking
type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

type InstrumentState struct {
	// --- Core Identity & Current Snapshots ---
	Symbol          string
	LastUpdated     time.Time
	LatestPrice     float64
	LiveSessionVWAP float64

	// --- VWAP Regime Counters ---
	ConsecutiveClosesAboveVwap int
	ConsecutiveClosesBelowVwap int

	// --- The Single Continuous Efficiency Tracker ---
	LatestPriceRank  int
	LatestVolumeRank int
	LatestChangePct  float64
	Efficiency       float64 // (PriceRank / VolumeRank) * CandleSign

	OpeningRangeHigh   float64 // 09:15-09:30 structural barrier
	OpeningRangeLow    float64 // 09:15-09:30 structural barrier
	OpeningRangeLocked bool
	VwapCrossCount     int // Measures structural chop intensity

	// --- Trade Tracking & Historical Buffers ---
	CurrentSetupPhase      SetupPhase
	LastTradedBarTime      time.Time // 🔒 The Chronological Execution Lock
	EntryVwapAnchor        float64
	NormalizedVwapDistance float64
	PeakVwapExtension      float64
	BarHistory             map[string][]*models.Bar
	Profile                *models.InstrumentProfile
}

// OptimizationTradeLog holds the freeze-frame microstructural variables
// required to analyze performance parameters cleanly.
type OptimizationTradeLog struct {
	ID                int       `json:"id" db:"id"`
	Symbol            string    `json:"symbol" db:"symbol"`
	StrategyName      string    `json:"strategy_name" db:"strategy_name"`
	TradeSide         string    `json:"trade_side" db:"trade_side"`
	MinutesSinceOpen  int       `json:"minutes_since_open" db:"minutes_since_open"`
	EntryTimestamp    time.Time `json:"entry_timestamp" db:"entry_timestamp"`
	EntryPrice        float64   `json:"entry_price" db:"entry_price"`
	EntryVwap         float64   `json:"entry_vwap" db:"entry_vwap"`
	EntryVolumeRank   int       `json:"entry_volume_rank" db:"entry_volume_rank"`
	EntryPriceRank    int       `json:"entry_price_rank" db:"entry_price_rank"`
	EntryWickRatio    float64   `json:"entry_wick_ratio" db:"entry_wick_ratio"`
	EntryVwapDistance float64   `json:"entry_vwap_distance" db:"entry_vwap_distance"`

	// 📸 Peak Capture Metrics
	PeakPnLINR float64 `json:"peak_pnl_inr" db:"peak_pnl_inr"` // ⚡ Captures maximum unrealized INR printed

	ExitTimestamp time.Time `json:"exit_timestamp" db:"exit_timestamp"`
	ExitPrice     float64   `json:"exit_price" db:"exit_price"`
	ExitReason    string    `json:"exit_reason" db:"exit_reason"`
	FinalPnLINR   float64   `json:"final_pnl_inr" db:"final_pnl_inr"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}
