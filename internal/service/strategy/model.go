package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

const (
	PhaseNeutral     = "NEUTRAL"
	PhaseActiveTrade = "ACTIVE_TRADE"
)

// StrategyStats tracks localized metrics for a specific strategy on a stock instrument
type StrategyStats struct {
	TradeCount        int       `json:"trade_count"`         // Total trades taken by this strategy for this stock today
	LastTradeTime     time.Time `json:"last_trade_time"`     // Timestamp of the last executed signal
	IsCurrentlyActive bool      `json:"is_currently_active"` // Whether this strategy has an open position context here
}

// TickResult packages the generated execution signal alongside its calculated state snapshot.
type TickResult struct {
	Signal string
	State  *InstrumentState
}

type InstrumentState struct {
	StockName          string
	Profile            *models.InstrumentProfile
	VwapPercentile     *models.VWAPDistancePercentile
	LatestPrice        float64
	LiveSessionVWAP    float64
	CurrentSetupPhase  string
	ActiveSide         string
	ActiveAvgPrice     float64
	CurrentTradeID     string
	CurrentPnL         float64
	PeakPnL            float64
	EntryVwapAnchor    float64
	EntryTimestamp     time.Time
	LastExitSignalTime time.Time
	LastTickTime       time.Time
	BarHistory         map[string][]*models.Bar

	SessionOpen float64
	SessionHigh float64
	SessionLow  float64

	ADRHigh float64
	ADRLow  float64

	ActiveStrategyName string                   `json:"active_strategy_name"` // e.g., "Combined_Mood_Velocity_Direct"
	StrategyHistory    map[string]StrategyStats `json:"strategy_history"`     // Tracks detailed historical metrics per strategy for this stock today
	StrategyTradeCount int                      `json:"strategy_trade_count"` // Global fallback counter or active tracking metric

	Metadata map[string]interface{} `json:"metadata,omitempty"`
}

// Clone constructs an isolated memory footprint copy to prevent side-effect leaks
func (s *InstrumentState) Clone() *InstrumentState {
	if s == nil {
		return nil
	}

	// 1. Create a deep copy of the BarHistory map
	clonedHistory := make(map[string][]*models.Bar)
	if s.BarHistory != nil {
		for tf, bars := range s.BarHistory {
			clonedBars := make([]*models.Bar, len(bars))
			copy(clonedBars, bars)
			clonedHistory[tf] = clonedBars
		}
	}

	// 2. 🌟 Create a deep copy of the StrategyHistory map with our new value type struct to prevent map race conditions
	clonedStrategyHistory := make(map[string]StrategyStats)
	if s.StrategyHistory != nil {
		for k, v := range s.StrategyHistory {
			clonedStrategyHistory[k] = v // Struct fields copy value-wise natively in Go assignment
		}
	}

	return &InstrumentState{
		StockName:          s.StockName,
		Profile:            s.Profile,
		VwapPercentile:     s.VwapPercentile,
		StrategyHistory:    clonedStrategyHistory,
		LatestPrice:        s.LatestPrice,
		LiveSessionVWAP:    s.LiveSessionVWAP,
		CurrentSetupPhase:  s.CurrentSetupPhase,
		ActiveSide:         s.ActiveSide,
		ActiveAvgPrice:     s.ActiveAvgPrice,
		CurrentTradeID:     s.CurrentTradeID,
		CurrentPnL:         s.CurrentPnL,
		PeakPnL:            s.PeakPnL,
		EntryVwapAnchor:    s.EntryVwapAnchor,
		EntryTimestamp:     s.EntryTimestamp,
		LastExitSignalTime: s.LastExitSignalTime,
		LastTickTime:       s.LastTickTime,
		BarHistory:         clonedHistory,
		SessionOpen:        s.SessionOpen,
		SessionHigh:        s.SessionHigh,
		SessionLow:         s.SessionLow,
		ADRHigh:            s.ADRHigh,
		ADRLow:             s.ADRLow,
		ActiveStrategyName: s.ActiveStrategyName,
		StrategyTradeCount: s.StrategyTradeCount,
	}
}
