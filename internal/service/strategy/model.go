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
}

// Clone constructs an isolated memory footprint copy to prevent side-effect leaks
func (s *InstrumentState) Clone() *InstrumentState {
	if s == nil {
		return nil
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
		BarHistory:         s.BarHistory, // Direct read reference safe for indicators
	}
}
