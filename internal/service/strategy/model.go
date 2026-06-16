package strategy

import (
	"gidh-backend/internal/service/models"
	"time"
)

const (
	PhaseNeutral     = "NEUTRAL"
	PhaseActiveTrade = "ACTIVE_TRADE"
)

// InstrumentState handles ONLY active trade execution lifecycle, position tracking,
// and shallow snapshots of analytical calculations forwarded by the pipeline stages.
type InstrumentState struct {
	StockName       string
	Profile         *models.InstrumentProfile
	LatestPrice     float64
	LiveSessionVWAP float64

	// Operational Trade Lifecycle State
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

	BarHistory map[string][]*models.Bar
}
