package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
)

type ScalperAgent struct {
	mu              sync.RWMutex
	enrichedBuffers map[string][]*models.EnrichedTick
	macroHorizons   map[string]map[string]*models.Bar
	// Memory tracking to prevent rapid-fire execution on the same candle
	lastTradedBarTime map[string]int64
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		enrichedBuffers:   make(map[string][]*models.EnrichedTick),
		macroHorizons:     make(map[string]map[string]*models.Bar),
		lastTradedBarTime: make(map[string]int64),
	}
}

func (sa *ScalperAgent) IngestClosedBar(bar *models.Bar) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.macroHorizons[bar.StockName] == nil {
		sa.macroHorizons[bar.StockName] = make(map[string]*models.Bar)
	}
	sa.macroHorizons[bar.StockName][bar.Timeframe] = bar
}

// ========================================================================
// 🏛️ MAIN ORCHESTRATOR
// ========================================================================

func (sa *ScalperAgent) AnalyzeMarket(enrichedTick *models.EnrichedTick, positionSide string) (string, bool) {
	symbol := enrichedTick.Raw.StockName

	// 1. Housekeeping: Update sliding window tick memory buffer
	sa.updateTickBuffer(symbol, enrichedTick)

	// 2. Context Retrieval: Fetch required closed bars and execution memory
	bar1m, bar5m, lastTradedTime, ok := sa.getMarketContext(symbol)
	if !ok {
		return "", false
	}

	// 3. Evaluate Position Exits
	if positionSide != "FLAT" && positionSide != "" {
		return sa.evaluateExitLogic(positionSide, bar1m)
	}

	// 4. Evaluate Position Entries (The Modular Pipeline)
	return sa.evaluateEntryPipeline(enrichedTick, bar1m, bar5m, lastTradedTime)
}

// ========================================================================
// 🛠️ SUB-MODULES & PIPELINE STAGES
// ========================================================================

// evaluateEntryPipeline runs structural setup and filter confirmation layers serially
func (sa *ScalperAgent) evaluateEntryPipeline(tick *models.EnrichedTick, bar1m *models.Bar, bar5m *models.Bar, lastTradedTime int64) (string, bool) {
	// Rule 1: Memory Guard (Debounce)
	if bar1m.Timestamp.UnixMilli() == lastTradedTime {
		return "", false
	}

	// Rule 2: Core Timeframe Setup Evaluation
	intent := sa.evaluateCoreSetup(bar1m)
	if intent == "" {
		return "", false
	}

	// Rule 3: Multi-Timeframe Trend Alignment Check
	if !sa.isTrendAligned(intent, bar5m) {
		return "", false
	}

	// Rule 4: Live Order Flow Confirmation Check
	if !sa.isLiveOrderFlowConfirmed(intent, tick) {
		return "", false
	}

	// Rule 5: Final Price Trigger Verification & Commitment Stamping
	return sa.executePriceTrigger(intent, bar1m, tick.Raw.LastPrice)
}

// getMarketContext cleanly extracts data and memory fields under a Read Lock
func (sa *ScalperAgent) getMarketContext(symbol string) (*models.Bar, *models.Bar, int64, bool) {
	sa.mu.RLock()
	defer sa.mu.RUnlock()

	macroMap, exists := sa.macroHorizons[symbol]
	if !exists || macroMap == nil {
		return nil, nil, 0, false
	}

	bar1m, ok1 := macroMap["1m"]
	bar5m, _ := macroMap["5m"] // Optional: Can be nil if 5m hasn't formed yet

	if !ok1 || bar1m == nil {
		return nil, nil, 0, false
	}

	return bar1m, bar5m, sa.lastTradedBarTime[symbol], true
}

// evaluateExitLogic isolates the technical exit checks based on the 1m bar
func (sa *ScalperAgent) evaluateExitLogic(positionSide string, bar1m *models.Bar) (string, bool) {
	if positionSide == "LONG" {
		if bar1m.Analytics.Direction == models.DirBearishAbsorption ||
			bar1m.Analytics.Direction == models.DirStrongBearish ||
			bar1m.Analytics.Direction == models.DirBearish {
			return "EXIT_LONG", true
		}
	} else if positionSide == "SHORT" {
		if bar1m.Analytics.Direction == models.DirBullishAbsorption ||
			bar1m.Analytics.Direction == models.DirStrongBullish ||
			bar1m.Analytics.Direction == models.DirBullish {
			return "EXIT_SHORT", true
		}
	}
	return "", false
}

// evaluateCoreSetup analyzes volatility context and returns trading intent
func (sa *ScalperAgent) evaluateCoreSetup(bar1m *models.Bar) string {
	isHighVolumeAbnormal := bar1m.Analytics.VolumeRank >= 7
	isPriceStretching := bar1m.Analytics.PriceRank >= 6

	if !isHighVolumeAbnormal || !isPriceStretching {
		return ""
	}

	if bar1m.Analytics.Direction == models.DirStrongBullish || bar1m.Analytics.Direction == models.DirBullish {
		return "INTENT_LONG"
	}
	if bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish {
		return "INTENT_SHORT"
	}

	return ""
}

// isTrendAligned filters signals that attempt to fight higher timeframe momentum
func (sa *ScalperAgent) isTrendAligned(intent string, bar5m *models.Bar) bool {
	if bar5m == nil {
		return true // Pass through if higher timeframe data isn't ready
	}

	if intent == "INTENT_LONG" {
		if bar5m.Analytics.Direction == models.DirStrongBearish || bar5m.Analytics.Direction == models.DirBearish {
			return false // Veto long if 5m is crashing
		}
	}
	if intent == "INTENT_SHORT" {
		if bar5m.Analytics.Direction == models.DirStrongBullish || bar5m.Analytics.Direction == models.DirBullish {
			return false // Veto short if 5m is ripping
		}
	}
	return true
}

// isLiveOrderFlowConfirmed watches live tick streams for absorption or fading risks
func (sa *ScalperAgent) isLiveOrderFlowConfirmed(intent string, enrichedTick *models.EnrichedTick) bool {
	// Since Enrichment is a value type, check if its structural direction is uninitialized
	if enrichedTick.Enrichment.Direction == "" {
		return true // Fallback: pass if enrichment telemetry data is empty
	}

	liveDirection := enrichedTick.Enrichment.Direction

	if intent == "INTENT_LONG" {
		if liveDirection == models.DirBearishAbsorption || liveDirection == models.DirStrongBearish {
			return false // Veto if aggressive limit sellers are actively blocking ticks
		}
	}
	if intent == "INTENT_SHORT" {
		if liveDirection == models.DirBullishAbsorption || liveDirection == models.DirStrongBullish {
			return false // Veto if aggressive limit buyers are propping up the floor
		}
	}
	return true
}

// executePriceTrigger ensures confirmation and logs the traded timestamp to memory state
func (sa *ScalperAgent) executePriceTrigger(intent string, bar1m *models.Bar, lastPrice float64) (string, bool) {
	symbol := bar1m.StockName

	if intent == "INTENT_LONG" && lastPrice >= bar1m.Close {
		sa.mu.Lock()
		sa.lastTradedBarTime[symbol] = bar1m.Timestamp.UnixMilli()
		sa.mu.Unlock()
		return "GO_LONG", true
	}

	if intent == "INTENT_SHORT" && lastPrice <= bar1m.Close {
		sa.mu.Lock()
		sa.lastTradedBarTime[symbol] = bar1m.Timestamp.UnixMilli()
		sa.mu.Unlock()
		return "GO_SHORT", true
	}

	return "", false
}
