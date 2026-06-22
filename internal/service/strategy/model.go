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

	// Create a deep copy of the BarHistory map
	clonedHistory := make(map[string][]*models.Bar)
	if s.BarHistory != nil {
		for tf, bars := range s.BarHistory {
			// Copy the slice slice reference (safe if slice elements aren't mutated concurrently)
			clonedBars := make([]*models.Bar, len(bars))
			copy(clonedBars, bars)
			clonedHistory[tf] = clonedBars
		}
	}

	return &InstrumentState{
		StockName:          s.StockName,
		Profile:            s.Profile,
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
		BarHistory:         clonedHistory, // Now truly isolated and thread-safe
	}
}
