package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

// Define the Absorption State Machine Phases
type SetupPhase string

const (
	PhaseNeutral                  SetupPhase = "NEUTRAL"
	PhaseBullishAbsorptionSpotted SetupPhase = "BULLISH_ABSORPTION_SPOTTED"
	PhaseBearishAbsorptionSpotted SetupPhase = "BEARISH_ABSORPTION_SPOTTED"
	PhaseActiveTrade              SetupPhase = "ACTIVE_TRADE"
)

// HistoricTickSnapshot only keeps what is needed for the 1-minute exit resolution memory.
type HistoricTickSnapshot struct {
	Timestamp time.Time
	Direction models.DirectionState
}

// InstrumentState is now hyper-lean, tracking only order flow state and bar history.
type InstrumentState struct {
	Symbol      string
	LastUpdated time.Time

	// Live State
	LatestPrice      float64
	LatestDirection  models.DirectionState
	LatestVolumeRank int

	// Memory (Used for Exits & Context)
	TimeQueue  []HistoricTickSnapshot
	BarHistory map[string][]*models.Bar

	// State Machine & Cooldown
	LastExitTime      time.Time
	CurrentSetupPhase SetupPhase
	PhaseTimestamp    time.Time
}

type ScalperAgent struct {
	mu           sync.RWMutex
	Registry     map[string]*InstrumentState
	TimeDuration time.Duration // Lookback for the time queue (e.g., 5 * time.Minute)
	BarDuration  time.Duration // Lookback for historical bars (e.g., 1 * time.Hour)
}

// NewScalperAgent creates the lean state manager. (Removed MaxTxCount).
func NewScalperAgent(timeDuration time.Duration, barDuration time.Duration) *ScalperAgent {
	return &ScalperAgent{
		Registry:     make(map[string]*InstrumentState),
		TimeDuration: timeDuration,
		BarDuration:  barDuration,
	}
}

// IngestClosedBar maintains the historical bar map for timeframe resolution checks.
func (sa *ScalperAgent) IngestClosedBar(bar *models.Bar) {
	if bar == nil {
		return
	}

	sa.mu.Lock()
	defer sa.mu.Unlock()

	symbol := bar.StockName
	state, exists := sa.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:     symbol,
			TimeQueue:  make([]HistoricTickSnapshot, 0, 100),
			BarHistory: make(map[string][]*models.Bar),
		}
		sa.Registry[symbol] = state
	}

	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	barCutoff := bar.Timestamp.Add(-sa.BarDuration)
	validIdx := 0
	for i, historicalBar := range state.BarHistory[tf] {
		if historicalBar.Timestamp.Before(barCutoff) {
			validIdx = i + 1
		} else {
			break
		}
	}
	if validIdx > 0 {
		state.BarHistory[tf] = state.BarHistory[tf][validIdx:]
	}
}

// UpdateMicroContext updates the live state and the rolling TimeQueue.
func (sa *ScalperAgent) UpdateMicroContext(enrichedTick *models.EnrichedTick) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	raw := enrichedTick.Raw
	symbol := raw.StockName

	state, exists := sa.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:     symbol,
			TimeQueue:  make([]HistoricTickSnapshot, 0, 100),
			BarHistory: make(map[string][]*models.Bar),
		}
		sa.Registry[symbol] = state
	}

	// 1. Update Live Variables
	state.LatestPrice = raw.LastPrice
	state.LastUpdated = raw.Timestamp
	state.LatestDirection = enrichedTick.Enrichment.Direction
	state.LatestVolumeRank = enrichedTick.Enrichment.VolumeRank

	// 2. Manage Rolling Time Memory (for Exits)
	snapshot := HistoricTickSnapshot{
		Timestamp: raw.Timestamp,
		Direction: enrichedTick.Enrichment.Direction,
	}
	state.TimeQueue = append(state.TimeQueue, snapshot)

	timeCutoff := raw.Timestamp.Add(-sa.TimeDuration)
	validIdx := 0
	for i, oldTick := range state.TimeQueue {
		if oldTick.Timestamp.Before(timeCutoff) {
			validIdx = i + 1
		} else {
			break
		}
	}
	if validIdx > 0 {
		state.TimeQueue = state.TimeQueue[validIdx:]
	}
}

// getRecentMinutesDataUnlocked fetches the rolling time window.
func (sa *ScalperAgent) getRecentMinutesDataUnlocked(state *InstrumentState, minutes int) []HistoricTickSnapshot {
	if state == nil || len(state.TimeQueue) == 0 || minutes <= 0 {
		return nil
	}

	latestTimestamp := state.TimeQueue[len(state.TimeQueue)-1].Timestamp
	cutoffTime := latestTimestamp.Add(-time.Duration(minutes) * time.Minute)

	validIdx := -1
	for i, tick := range state.TimeQueue {
		if !tick.Timestamp.Before(cutoffTime) {
			validIdx = i
			break
		}
	}

	if validIdx == -1 {
		return nil
	}

	relevantData := state.TimeQueue[validIdx:]
	result := make([]HistoricTickSnapshot, len(relevantData))
	copy(result, relevantData)

	return result
}
