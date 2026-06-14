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

type InstitutionalLedger struct {
	BullEfficient float64   `json:"bull_efficient"`
	BearEfficient float64   `json:"bear_efficient"`
	LastUpdated   time.Time `json:"last_updated"`
}

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

	// --- 📈 Dynamic Position PnL Fields ---
	CurrentPnL float64 `json:"current_pnl"`
	PeakPnL    float64 `json:"peak_pnl"`

	// --- Asset Context Ranks ---
	LatestPriceRank  int `json:"latest_price_rank"`
	LatestVolumeRank int `json:"latest_volume_rank"`

	// --- 📊 Expanded Memory Fields (Fixes Bug #1 & #2) ---
	Ledger               InstitutionalLedger `json:"ledger"`
	NetEfficiency        float64             `json:"net_efficiency"`         // [-150 to 150 Scale]
	NetEfficiencySlope   float64             `json:"net_efficiency_slope"`   // 10-bar Trend Quality line
	NetEfficiencyHistory []float64           `json:"net_efficiency_history"` // Cached trailing row buffer for slope
	PeakEfficiency       float64             `json:"peak_efficiency"`        // 🔒 Safely isolated peak cache container

	// --- Trade Tracking & Historical Buffers ---
	CurrentSetupPhase SetupPhase                `json:"current_setup_phase"`
	LastTradedBarTime time.Time                 `json:"last_traded_bar_time"`
	EntryVwapAnchor   float64                   `json:"entry_vwap_anchor"`
	BarHistory        map[string][]*models.Bar  `json:"bar_history"`
	Profile           *models.InstrumentProfile `json:"profile"`
}

// OptimizationTradeLog fully updated with Missing Metrics (Fixes Bug #8)
type OptimizationTradeLog struct {
	ID                int       `json:"id" db:"id"`
	Symbol            string    `json:"symbol" db:"symbol"`
	StrategyName      string    `json:"strategy_name" db:"strategy_name"`
	TradeSide         string    `json:"trade_side" db:"trade_side"`
	MinutesSinceOpen  int       `json:"minutes_since_open" db:"minutes_since_open"`
	EntryTimestamp    time.Time `json:"entry_timestamp" db:"entry_timestamp"`
	EntryPrice        float64   `json:"entry_price" db:"entry_price"`
	EntryVwap         float64   `json:"entry_vwap" db:"entry_vwap"`
	EntryVwapDistance float64   `json:"entry_vwap_distance" db:"entry_vwap_distance"`

	// Analytics Snapshots
	EntryEfficiency float64 `json:"entry_efficiency" db:"entry_efficiency"`
	EntryDelta      float64 `json:"entry_delta" db:"entry_delta"`
	EntrySlope      float64 `json:"entry_slope" db:"entry_slope"`
	EntryVolumeRank int     `json:"entry_volume_rank" db:"entry_volume_rank"`

	PeakPnLINR             float64 `json:"peak_pnl_inr" db:"peak_pnl_inr"`
	EfficiencyCaptureRatio float64 `json:"efficiency_capture_ratio" db:"efficiency_capture_ratio"` // 📈 Added optimization vector

	ExitTimestamp time.Time `json:"exit_timestamp" db:"exit_timestamp"`
	ExitPrice     float64   `json:"exit_price" db:"exit_price"`
	ExitReason    string    `json:"exit_reason" db:"exit_reason"`
	FinalPnLINR   float64   `json:"final_pnl_inr" db:"final_pnl_inr"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
}
