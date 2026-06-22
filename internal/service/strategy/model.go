package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

const (
	PhaseNeutral     = "NEUTRAL"
	PhaseActiveTrade = "ACTIVE_TRADE"
)

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

	ActiveStrategyName string          `json:"active_strategy_name"` // e.g., "Combined_Mood_Velocity_Direct"
	StrategyHistory    map[string]bool `json:"strategy_history"`     // Tracks which strategies have already traded this stock today
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

	// 2. 🌟 Create a deep copy of the StrategyHistory map to prevent map race conditions
	clonedStrategyHistory := make(map[string]bool)
	if s.StrategyHistory != nil {
		for k, v := range s.StrategyHistory {
			clonedStrategyHistory[k] = v
		}
	}

	return &InstrumentState{
		StockName:          s.StockName,
		Profile:            s.Profile,
		VwapPercentile:     s.VwapPercentile,      // 🌟 Fixed: Pass the baseline percentile parameters
		StrategyHistory:    clonedStrategyHistory, // 🌟 Fixed: Pass the tracking history map safely
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
	}
}
