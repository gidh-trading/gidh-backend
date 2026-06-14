package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

// SetupPhase defines the state machine flags for active execution tracking.
type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

// InstrumentState represents the ultra-lean runtime context engine for an individual asset.
type InstrumentState struct {
	// --- Core Identity & Current Snapshots ---
	Symbol          string    `json:"symbol"`
	LastUpdated     time.Time `json:"last_updated"`
	LatestPrice     float64   `json:"latest_price"`
	LiveSessionVWAP float64   `json:"live_session_vwap"`

	// --- VWAP Regime Counters & Core Metric Vectors ---
	ConsecutiveClosesAboveVwap int     `json:"consecutive_closes_above_vwap"`
	ConsecutiveClosesBelowVwap int     `json:"consecutive_closes_below_vwap"`
	TimePctAboveVwap           float64 `json:"time_pct_above_vwap"`
	NormalizedVwapDistance     float64 `json:"normalized_vwap_distance"`
	TotalSessionBars           int     `json:"total_session_bars"`
	LatestChangePct            float64 `json:"latest_change_pct"`

	// --- 📊 Simplified Microstructural Core Indicators ---
	NetEfficiency        float64   `json:"net_efficiency"`         // Consolidated pure institutional net footprints
	NetEfficiencySlope   float64   `json:"net_efficiency_slope"`   // Linear regression rate of change over recent history
	NetEfficiencyHistory []float64 `json:"net_efficiency_history"` // Cached trailing rolling window used for slope calculations

	// --- Trade Tracking & Historical Buffers ---
	CurrentSetupPhase SetupPhase                `json:"current_setup_phase"`
	LastTradedBarTime time.Time                 `json:"last_traded_bar_time"` // 🔒 The Chronological Execution Lock
	EntryVwapAnchor   float64                   `json:"entry_vwap_anchor"`
	PeakVwapExtension float64                   `json:"peak_vwap_extension"`
	BarHistory        map[string][]*models.Bar  `json:"bar_history"`
	Profile           *models.InstrumentProfile `json:"profile"`
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
