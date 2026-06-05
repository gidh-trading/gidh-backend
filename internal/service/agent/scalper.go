package agent

import (
	"time"

	"gidh-backend/internal/service/models"
)

type ScalperAgent struct {
	enrichedBuffers map[string][]*models.EnrichedTick
	macroHorizons   map[string]map[string]*models.Bar
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		enrichedBuffers: make(map[string][]*models.EnrichedTick),
		macroHorizons:   make(map[string]map[string]*models.Bar),
	}
}

// IngestClosedBar caches timeframe intervals when a candle finishes compiling
func (sa *ScalperAgent) IngestClosedBar(bar *models.Bar) {
	if sa.macroHorizons[bar.StockName] == nil {
		sa.macroHorizons[bar.StockName] = make(map[string]*models.Bar)
	}
	sa.macroHorizons[bar.StockName][bar.Timeframe] = bar
}

// AnalyzeMarket evaluates the 60s micro tick window and macro bars to return strategic directions
func (sa *ScalperAgent) AnalyzeMarket(enrichedTick *models.EnrichedTick, positionSide string) (string, bool) {
	raw := enrichedTick.Raw
	symbol := raw.StockName

	// 1. Engineering: Maintain sliding 60-second rolling buffer of ENRICHED ticks
	buffer := sa.enrichedBuffers[symbol]
	cutoff := raw.Timestamp.Add(-60 * time.Second)

	validIdx := 0
	for i, t := range buffer {
		if t.Raw.Timestamp.After(cutoff) {
			validIdx = i
			break
		}
	}
	if len(buffer) > 0 && buffer[validIdx].Raw.Timestamp.After(cutoff) {
		buffer = buffer[validIdx:]
	} else if len(buffer) > 0 {
		buffer = []*models.EnrichedTick{}
	}
	buffer = append(buffer, enrichedTick)
	sa.enrichedBuffers[symbol] = buffer

	// 2. Analytics: Load finalized macro bar boundaries for context
	macroMap, exists := sa.macroHorizons[symbol]
	if !exists || macroMap == nil {
		return "", false
	}
	bar1m, ok := macroMap["1m"]
	if !ok || bar1m == nil {
		return "", false
	}

	// 3. Strategy Logic: Use the rich indicators already computed inside enrichedTick!
	if positionSide == "FLAT" || positionSide == "" {
		// Example: Now you can directly look at advanced indicators inside enrichedTick
		if bar1m.Analytics.VolumeRank >= 6 && len(buffer) > 15 {
			return "GO_LONG", true
		}
	} else if positionSide == "LONG" {
		if bar1m.Analytics.Direction == models.DirBearishAbsorption {
			return "EXIT_LONG", true
		}
	}

	return "", false
}
