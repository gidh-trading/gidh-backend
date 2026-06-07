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
	// NEW: HISTORICAL TIME-FRAME BARS STATE MAP (Up to 1 hour rolling window)
	// ========================================================================
	BarHistory map[string][]*models.Bar

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
	BarDuration  time.Duration // LOOKBACK BOUNDARY FOR HISTORICAL TIMEFRAME BARS (e.g., 1 * time.Hour)
}

// NewScalperAgent creates and initializes the time-independent engineering storage manager
// Now properly supports 3 configuration arguments to match pipeline setup logic.
func NewScalperAgent(maxTxCount int, timeDuration time.Duration, barDuration time.Duration) *ScalperAgent {
	return &ScalperAgent{
		Registry:     make(map[string]*InstrumentState),
		MaxTxCount:   maxTxCount,
		TimeDuration: timeDuration,
		BarDuration:  barDuration,
	}
}

// IngestClosedBar implements a clean strategy channel interface compatible with BarManager hooks.
// It keeps tracks of past hours of historical bars and prunes outdated buckets.
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
			TxQueue:    make([]HistoricTickSnapshot, 0, sa.MaxTxCount),
			TimeQueue:  make([]HistoricTickSnapshot, 0, 100),
			BarHistory: make(map[string][]*models.Bar),
		}
		sa.Registry[symbol] = state
	}

	if state.BarHistory == nil {
		state.BarHistory = make(map[string][]*models.Bar)
	}

	tf := bar.Timeframe
	state.BarHistory[tf] = append(state.BarHistory[tf], bar)

	// Keep rolling historical lookback within parameters (1 hour sliding cutoff window)
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

// UpdateMicroContext updates both transaction and time-bounded queues simultaneously per tick.
func (sa *ScalperAgent) UpdateMicroContext(enrichedTick *models.EnrichedTick) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	raw := enrichedTick.Raw
	symbol := raw.StockName

	state, exists := sa.Registry[symbol]
	if !exists {
		state = &InstrumentState{
			Symbol:     symbol,
			TxQueue:    make([]HistoricTickSnapshot, 0, sa.MaxTxCount),
			TimeQueue:  make([]HistoricTickSnapshot, 0, 100),
			BarHistory: make(map[string][]*models.Bar),
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

	state.LatestVolumeRank = enrichedTick.Enrichment.VolumeRank
	state.LatestPriceRank = enrichedTick.Enrichment.PriceRank
	state.LatestRangeRank = enrichedTick.Enrichment.RangeRank
	state.LatestTickRank = enrichedTick.Enrichment.TickRank
	state.LatestDirection = enrichedTick.Enrichment.Direction

	volRank := enrichedTick.Enrichment.VolumeRank
	priceRank := enrichedTick.Enrichment.PriceRank
	rangeRank := enrichedTick.Enrichment.RangeRank
	tickRank := enrichedTick.Enrichment.TickRank

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
func (sa *ScalperAgent) GenerateSignal(symbol string, currentSide string, entryPrice float64) string {
	sa.mu.RLock()
	state, exists := sa.Registry[symbol]
	sa.mu.RUnlock()

	if !exists || len(state.TxQueue) == 0 || len(state.TimeQueue) == 0 {
		return "HOLD"
	}

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

	morningSignal := sa.generateMorningSignal(state, currentSide)
	if morningSignal != "HOLD" {
		return morningSignal
	}

	return "HOLD"
}
