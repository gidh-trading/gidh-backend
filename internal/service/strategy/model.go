package strategy

import (
	"time"

	"gidh-backend/internal/service/models"
)

const (
	PhaseNeutral     = "NEUTRAL"
	PhaseActiveTrade = "ACTIVE_TRADE"
)

// InstrumentState handles ONLY active trade execution lifecycle, position tracking,
// and shallow snapshots of analytical calculations forwarded by the pipeline stages.
type InstrumentState struct {
	StockName              string
	Profile                *models.InstrumentProfile
	LatestPrice            float64
	LiveSessionVWAP        float64
	NormalizedVwapDistance float64

	// Operational Trade Lifecycle State (Owned entirely by strategy.Engine)
	CurrentSetupPhase string  // e.g., PhaseNeutral, PhaseActiveTrade
	CurrentPnL        float64 // Real-time run tracking currency delta
	PeakPnL           float64 // Maximum unrealized PnL reached during active position
	EntryVwapAnchor   float64 // VWAP level recorded at the exact point of market entry

	BarHistory map[string][]*models.Bar // key: Timeframe (e.g., "1m", "5m")
}

// OptimizationTradeLog records the immutable entry baseline footprint metrics
// and captures trade execution outcome performance details.
type OptimizationTradeLog struct {
	Symbol                 string    `json:"symbol"`
	StrategyName           string    `json:"strategy_name"`
	TradeSide              string    `json:"trade_side"`
	EntryTimestamp         time.Time `json:"entry_timestamp"`
	EntryPrice             float64   `json:"entry_price"`
	EntryVwap              float64   `json:"entry_vwap"`
	EntryVwapDistance      float64   `json:"entry_vwap_distance"`
	EntryEfficiency        float64   `json:"entry_efficiency"`
	EntryDelta             float64   `json:"entry_delta"`
	EntrySlope             float64   `json:"entry_slope"`
	EntryVolumeRank        int       `json:"entry_volume_rank"`
	ExitTimestamp          time.Time `json:"exit_timestamp"`
	ExitPrice              float64   `json:"exit_price"`
	ExitReason             string    `json:"exit_reason"`
	FinalPnLINR            float64   `json:"final_pnl_inr"`
	PeakPnLINR             float64   `json:"peak_pnl_inr"`
	EfficiencyCaptureRatio float64   `json:"efficiency_capture_ratio"`
	CreatedAt              time.Time `json:"created_at"`
}
