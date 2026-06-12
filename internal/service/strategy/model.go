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
// internal/service/strategy/model.go

type InstrumentState struct {
	Symbol          string
	LastUpdated     time.Time
	LatestPrice     float64
	LiveSessionVWAP float64

	// --- 🗺️ Daily Opening Landscape Context ---
	IsGapUp            bool
	IsGapDown          bool
	InitialOpenPrice   float64
	EntryVwapAnchor    float64
	HasInitializedGaps bool

	// --- 📊 VWAP Live Acceptance Tracking ---
	ConsecutiveClosesAboveVwap int
	ConsecutiveClosesBelowVwap int
	IsVwapAcceptanceConfirmed  bool

	// --- 🥊 The Institutional Ledger (Tug of War) ---
	BullishPushVolume float64
	BearishPushVolume float64

	// --- 🔄 Real-Time Spatial Snapshots ---
	LatestVolumeRank       int
	LatestPriceRank        int
	LatestEfficiency       float64 // ⚡ Added: Measures PriceRank / VolumeRank
	PreviousVolumeRank     int     // ⚡ Added: Retains previous closed bar volume rank
	PreviousPriceRank      int     // ⚡ Added: Retains previous closed bar price rank
	PreviousEfficiency     float64 // ⚡ Added: Retains previous closed bar efficiency
	NormalizedVwapDistance float64
	PeakVwapExtension      float64

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase
	LastTradedBarTime time.Time
	BarHistory        map[string][]*models.Bar
	Profile           *models.InstrumentProfile
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
