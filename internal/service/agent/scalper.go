package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

// HistoricTickSnapshot captures a clean state of an individual transaction element
type HistoricTickSnapshot struct {
	Timestamp   time.Time
	Price       float64
	Volume      float64
	SessionVWAP float64
	VolumeRank  int
	PriceRank   int
	TickRank    int
	RangeRank   int
	Direction   models.DirectionState
	ChangePct   float64
}

// InstrumentState serves as the engineering data vault for a single asset ticker.
type InstrumentState struct {
	Symbol      string
	LastUpdated time.Time

	// Microscopic Live Scalar Trackers (Latest streaming tick info)
	LatestPrice             float64
	LatestSessionVWAP       float64
	LatestChangePct         float64
	LatestVolumeRank        int
	LatestPriceRank         int
	LatestRangeRank         int
	LatestTickRank          int
	LatestDirection         models.DirectionState
	LatestTotalBuyQuantity  int64
	LatestTotalSellQuantity int64

	// QUEUE 1: Transaction-Based Rolling Window Memory (Fixed Count)
	TxQueue []HistoricTickSnapshot

	// QUEUE 2: Time-Based Rolling Window Memory (Fluid Count, Fixed Duration)
	TimeQueue []HistoricTickSnapshot

	// ========================================================================
	// NEW STRUCTURAL METRICS FOR MORNING STRATEGY
	// ========================================================================
	OpeningRangeSet bool    // Has the 9:15-9:20 range been calculated yet?
	OpeningHigh     float64 // Highest price printed between 9:15 and 9:20 AM
	OpeningLow      float64 // Lowest price printed between 9:15 and 9:20 AM

	LastExitTime    time.Time // Timestamp of the last position closure (for Cooldown)
	PrevSessionVWAP float64   // Used to measure the directional slope of VWAP over time
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
	state.LatestSessionVWAP = raw.AverageTradedPrice
	state.LatestChangePct = raw.Change
	state.LastUpdated = raw.Timestamp
	state.LatestTotalBuyQuantity = raw.TotalBuyQuantity
	state.LatestTotalSellQuantity = raw.TotalSellQuantity

	volRank := 0
	priceRank := 0
	rangeRank := 0
	tickRank := 0

	state.LatestVolumeRank = enrichedTick.Enrichment.VolumeRank
	state.LatestPriceRank = enrichedTick.Enrichment.PriceRank
	state.LatestRangeRank = enrichedTick.Enrichment.RangeRank
	state.LatestTickRank = enrichedTick.Enrichment.TickRank
	state.LatestDirection = enrichedTick.Enrichment.Direction

	volRank = enrichedTick.Enrichment.VolumeRank
	priceRank = enrichedTick.Enrichment.PriceRank
	rangeRank = enrichedTick.Enrichment.RangeRank
	tickRank = enrichedTick.Enrichment.TickRank

	// Unpack volume fields (Using LastQuantity safely)
	vol := float64(enrichedTick.TickVolume)
	if vol <= 0 {
		vol = 1.0 // Safety normalization fallback for zero baseline quantity prints
	}

	// 2. Assemble historical snapshot element frame
	snapshot := HistoricTickSnapshot{
		Timestamp:   raw.Timestamp,
		Price:       raw.LastPrice,
		Volume:      vol,
		SessionVWAP: raw.AverageTradedPrice,
		VolumeRank:  volRank,
		PriceRank:   priceRank,
		RangeRank:   rangeRank,
		TickRank:    tickRank,
		Direction:   enrichedTick.Enrichment.Direction,
		ChangePct:   raw.Change,
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

// GenerateSignal handles the primary routing logic for engineering direction signals.
// It serves as the main entry point that will evaluate our suite of trading strategies.
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	state, exists := sa.Registry[symbol]
	sa.mu.RUnlock()

	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

	// ------------------------------------------------------------------------
	// STEP 1: GLOBAL CAPITAL EMERGENCY RISK SHIELD
	// ------------------------------------------------------------------------
	if currentSide != "FLAT" && currentSide != "" {
		if sa.CheckGlobalEmergencyBrackets(state, entryPrice, currentSide) {
			if currentSide == "SHORT" {
				return "EXIT_SHORT"
			}
			if currentSide == "LONG" {
				return "EXIT_LONG"
			}
		}
	}

	// ------------------------------------------------------------------------
	// STEP 2: STRATEGY ROUTING PIPELINE
	// ------------------------------------------------------------------------

	// Execute the Morning Momentum Flow (Dark Age) Strategy
	morningSignal := sa.generateMorningSignal(state, currentSide)
	if morningSignal != "HOLD" {
		return morningSignal
	}

	// Future strategy modules can be sequentially evaluated right here:
	// afternoonSignal := sa.generateAfternoonSignal(state, currentSide)
	// if afternoonSignal != "HOLD" { return afternoonSignal }

	return "HOLD"
}
