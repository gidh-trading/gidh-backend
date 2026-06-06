package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

// HistoricTickSnapshot captures a clean state of an individual transaction element
type HistoricTickSnapshot struct {
	Timestamp  time.Time
	Price      float64
	Volume     float64
	VolumeRank int
	Direction  models.DirectionState
	ChangePct  float64
}

// InstrumentState serves as the engineering data vault for a single asset ticker.
type InstrumentState struct {
	Symbol      string
	LastUpdated time.Time

	// Microscopic Live Scalar Trackers (Latest streaming tick info)
	LatestPrice      float64
	LatestChangePct  float64
	LatestVolumeRank int
	LatestDirection  models.DirectionState

	// QUEUE 1: Transaction-Based Rolling Window Memory (Fixed Count)
	TxQueue []HistoricTickSnapshot

	// QUEUE 2: Time-Based Rolling Window Memory (Fluid Count, Fixed Duration)
	TimeQueue []HistoricTickSnapshot
}

type ScalperAgent struct {
	mu           sync.RWMutex
	Registry     map[string]*InstrumentState
	MaxTxCount   int           // Max depth cap for the transaction queue (e.g., 50 ticks)
	TimeDuration time.Duration // Lookback duration boundary for the time queue (e.g., 5 * time.Minute)
}

// NewScalperAgent creates and initializes the time-independent engineering storage manager
func NewScalperAgent(maxTxCount int, timeDuration time.Duration) *ScalperAgent {
	return &ScalperAgent{
		Registry:     make(map[string]*InstrumentState),
		MaxTxCount:   maxTxCount,
		TimeDuration: timeDuration,
	}
}

// UpdateMicroContext updates both transaction and time-bounded queues simultaneously per tick.
func (sa *ScalperAgent) UpdateMicroContext(enrichedTick *models.EnrichedTick) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	raw := enrichedTick.Raw
	symbol := raw.StockName

	state, exists := sa.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:    symbol,
			TxQueue:   make([]HistoricTickSnapshot, 0, sa.MaxTxCount),
			TimeQueue: make([]HistoricTickSnapshot, 0, 100), // Optimal capacity estimation block
		}
		sa.Registry[symbol] = state
	}

	// 1. Ingest immediate live scalar lookups
	state.LatestPrice = raw.LastPrice
	state.LatestChangePct = raw.Change
	state.LastUpdated = time.Now()

	volRank := 0
	state.LatestVolumeRank = enrichedTick.Enrichment.VolumeRank
	state.LatestDirection = enrichedTick.Enrichment.Direction
	volRank = enrichedTick.Enrichment.VolumeRank

	// Unpack volume fields (Using LastQuantity safely)
	vol := float64(enrichedTick.TickVolume)
	if vol <= 0 {
		vol = 1.0 // Safety normalization fallback for zero baseline quantity prints
	}

	// 2. Assemble historical snapshot element frame
	snapshot := HistoricTickSnapshot{
		Timestamp:  raw.Timestamp,
		Price:      raw.LastPrice,
		Volume:     vol,
		VolumeRank: volRank,
		Direction:  enrichedTick.Enrichment.Direction,
		ChangePct:  raw.Change,
	}

	// ------------------------------------------------------------------------
	// PROCESSING QUEUE 1: TRANSACTION-BASED WINDOW MANAGEMENT (FIXED COUNT)
	// ------------------------------------------------------------------------
	state.TxQueue = append(state.TxQueue, snapshot)
	if len(state.TxQueue) > sa.MaxTxCount {
		state.TxQueue = state.TxQueue[1:]
	}

	// ------------------------------------------------------------------------
	// PROCESSING QUEUE 2: TIME-BASED WINDOW MANAGEMENT (FIXED DURATION)
	// ------------------------------------------------------------------------
	state.TimeQueue = append(state.TimeQueue, snapshot)

	// Drop historical tick elements outside your window boundary (e.g., older than 5 minutes)
	timeCutoff := raw.Timestamp.Add(-sa.TimeDuration)

	validIdx := 0
	for i, oldTick := range state.TimeQueue {
		if oldTick.Timestamp.Before(timeCutoff) {
			validIdx = i + 1
		} else {
			break // Chronological packet ordering guarantees trailing entries fall inside parameters
		}
	}
	if validIdx > 0 {
		state.TimeQueue = state.TimeQueue[validIdx:]
	}
}

func (sa *ScalperAgent) getLastTransactionsUnlocked(state *InstrumentState, count int) []HistoricTickSnapshot {
	if state == nil || len(state.TxQueue) == 0 || count <= 0 {
		return nil
	}

	totalElements := len(state.TxQueue)
	if count > totalElements {
		count = totalElements
	}

	startIndex := totalElements - count
	result := make([]HistoricTickSnapshot, count)
	copy(result, state.TxQueue[startIndex:])

	return result
}

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

// ============================================================================
// PUBLIC THREAD-SAFE API (Use these from other packages/external components)
// ============================================================================

func (sa *ScalperAgent) GetLastTransactions(symbol string, count int) []HistoricTickSnapshot {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	state, exists := sa.Registry[symbol]
	if !exists {
		return nil
	}
	return sa.getLastTransactionsUnlocked(state, count)
}

func (sa *ScalperAgent) GetRecentMinutesData(symbol string, minutes int) []HistoricTickSnapshot {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	state, exists := sa.Registry[symbol]
	if !exists {
		return nil
	}
	return sa.getRecentMinutesDataUnlocked(state, minutes)
}
