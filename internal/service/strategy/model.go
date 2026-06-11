package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

// InstrumentState tracks stable, macro-structural session context instead of frantic speed.
type InstrumentState struct {
	Symbol          string
	LastUpdated     time.Time
	LatestPrice     float64
	LiveSessionVWAP float64 // The ultimate anchor line from the exchange

	// --- 🗺️ Daily Opening Landscape Context ---
	IsGapUp            bool    // Locked via first tick change percent
	IsGapDown          bool    // Locked via first tick change percent
	InitialOpenPrice   float64 // Captured from the first 1-minute bar of the session
	EntryVwapAnchor    float64 // Captures and freezes the exact VWAP price at the moment of entry
	HasInitializedGaps bool    // Tracker flag to freeze opening context

	// --- 📊 VWAP Live Acceptance Tracking ---
	ConsecutiveClosesAboveVwap int  // Rolling block tracker of sustained presence over anchor
	ConsecutiveClosesBelowVwap int  // Rolling block tracker of sustained presence under anchor
	IsVwapAcceptanceConfirmed  bool // Flips true when trend dominance is mathematically confirmed

	// --- 🥊 The Institutional Ledger (Tug of War) ---
	// Accumulates absolute volume traded on high-conviction, directional bars
	BullishPushVolume float64 // Absolute shares committed to attacking the offer
	BearishPushVolume float64 // Absolute shares committed to slamming the bid

	// --- 🔄 Real-Time Spatial Snapshots ---
	LatestVolumeRank       int     // Captured from incoming closed bar metrics
	LatestPriceRank        int     // Percentage representation of body size
	NormalizedVwapDistance float64 // Distance from VWAP scaled by ADRPct
	PeakVwapExtension      float64 // Maximum absolute distance reached during active trade tracking

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase
	LastTradedBarTime time.Time
	BarHistory        map[string][]*models.Bar  // Holds historical closed bars
	Profile           *models.InstrumentProfile // Stores ADRPct and ADV constants
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
