package strategy

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

type SetupPhase string

const (
	PhaseNeutral     SetupPhase = "NEUTRAL"
	PhaseActiveTrade SetupPhase = "ACTIVE_TRADE"
)

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
	NormalizedVwapDistance float64
	PeakVwapExtension      float64

	// --- 🎯 Execution State Machine ---
	CurrentSetupPhase SetupPhase
	LastTradedBarTime time.Time
	BarHistory        map[string][]*models.Bar
	Profile           *models.InstrumentProfile
}

type Engine struct {
	mu               sync.RWMutex
	Registry         map[string]*InstrumentState
	ActiveStrategy   Strategy
	MaxBarLookback   time.Duration
	profiles         map[string]*models.InstrumentProfile
	ActiveTrades     map[string]*OptimizationTradeLog
	OnTradeCompleted func(log *OptimizationTradeLog)
}
