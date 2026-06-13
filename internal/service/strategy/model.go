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

// LedgerState categorizes the structural footprint left by institutional participants on a closed bar.
type LedgerState string

const (
	StateEfficientBull  LedgerState = "EFFICIENT_BULL"
	StateEfficientBear  LedgerState = "EFFICIENT_BEAR"
	StateBullAbsorption LedgerState = "BULLISH_ABSORPTION"
	StateBearAbsorption LedgerState = "BEARISH_ABSORPTION"
	StateBullVacuum     LedgerState = "BULLISH_VACUUM"
	StateBearVacuum     LedgerState = "BEARISH_VACUUM"
	StateUndetermined   LedgerState = "UNDETERMINED"
)

// InstitutionalLedger tracks cumulative institutional pressure via continuous decay (Memory Context).
type InstitutionalLedger struct {
	BullEfficient  float64   `json:"bull_efficient"`
	BearEfficient  float64   `json:"bear_efficient"`
	BullAbsorption float64   `json:"bull_absorption"`
	BearAbsorption float64   `json:"bear_absorption"`
	BullVacuum     float64   `json:"bull_vacuum"`
	BearVacuum     float64   `json:"bear_vacuum"`
	LastUpdated    time.Time `json:"last_updated"`
}

// MicroContext preserves a pristine sequence of immediate past bar behaviors (Tactical Trigger).
type MicroContext struct {
	RecentStates []LedgerState `json:"recent_states"` // Sliced rolling window bound to TriggerLookback (e.g., last 3 bars)
}

// InstrumentState represents the runtime context engine for an individual asset.
type InstrumentState struct {
	// --- Core Identity & Current Snapshots ---
	Symbol          string    `json:"symbol"`
	LastUpdated     time.Time `json:"last_updated"`
	LatestPrice     float64   `json:"latest_price"`
	LiveSessionVWAP float64   `json:"live_session_vwap"`

	// --- VWAP Regime Counters ---
	ConsecutiveClosesAboveVwap int `json:"consecutive_closes_above_vwap"`
	ConsecutiveClosesBelowVwap int `json:"consecutive_closes_below_vwap"`

	// --- The Single Continuous Efficiency Tracker ---
	LatestPriceRank  int     `json:"latest_price_rank"`
	LatestVolumeRank int     `json:"latest_volume_rank"`
	LatestChangePct  float64 `json:"latest_change_pct"`
	Efficiency       float64 `json:"efficiency"` // (PriceRank / VolumeRank) * CandleSign

	OpeningRangeHigh   float64 `json:"opening_range_high"` // 09:15-09:30 structural barrier
	OpeningRangeLow    float64 `json:"opening_range_low"`  // 09:15-09:30 structural barrier
	OpeningRangeLocked bool    `json:"opening_range_locked"`
	VwapCrossCount     int     `json:"vwap_cross_count"` // Measures structural chop intensity

	// --- 📊 NEW STRUCTURAL MEMORY & TRIGGER BLOCKS ---
	Ledger         InstitutionalLedger `json:"ledger"`
	TriggerContext MicroContext        `json:"trigger_context"`

	// --- Trade Tracking & Historical Buffers ---
	CurrentSetupPhase      SetupPhase                `json:"current_setup_phase"`
	LastTradedBarTime      time.Time                 `json:"last_traded_bar_time"` // 🔒 The Chronological Execution Lock
	EntryVwapAnchor        float64                   `json:"entry_vwap_anchor"`
	NormalizedVwapDistance float64                   `json:"normalized_vwap_distance"`
	PeakVwapExtension      float64                   `json:"peak_vwap_extension"`
	BarHistory             map[string][]*models.Bar  `json:"bar_history"`
	Profile                *models.InstrumentProfile `json:"profile"`
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
