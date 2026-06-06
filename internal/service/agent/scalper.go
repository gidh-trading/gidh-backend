package agent

import (
	"sync"

	"gidh-backend/internal/service/models"
)

type ScalperAgent struct {
	mu            sync.RWMutex
	macroHorizons map[string]map[string]*models.Bar
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		macroHorizons: make(map[string]map[string]*models.Bar),
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

func (sa *ScalperAgent) AnalyzeMarket(enrichedTick *models.EnrichedTick, positionSide string) (string, bool) {
	raw := enrichedTick.Raw
	symbol := raw.StockName

	// 1. Extract the macro 1m candle context
	sa.mu.RLock()
	macroMap, exists := sa.macroHorizons[symbol]
	if !exists || macroMap == nil {
		sa.mu.RUnlock()
		return "", false
	}
	bar1m, ok := macroMap["1m"]
	if !ok || bar1m == nil {
		sa.mu.RUnlock()
		return "", false
	}
	sa.mu.RUnlock()

	// 2. Simplified Execution Engine
	switch positionSide {
	case "FLAT", "":
		// ENTRY: Is the closed macro candle a high-velocity momentum breakout?
		isHighVolume := bar1m.Analytics.VolumeRank >= 6
		if isHighVolume {
			// Ride the momentum in the direction of the macro bar
			if bar1m.Analytics.Direction == models.DirStrongBullish || bar1m.Analytics.Direction == models.DirBullish {
				return "GO_LONG", true
			}
			if bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish {
				return "GO_SHORT", true
			}
		}

	case "LONG":
		// EXIT LONG: Exit only if structural absorption confirms buyers are trapped
		if bar1m.Analytics.Direction == models.DirBearishAbsorption {
			return "EXIT_LONG", true
		}

	case "SHORT":
		// EXIT SHORT: Exit only if structural absorption confirms sellers are trapped
		if bar1m.Analytics.Direction == models.DirBullishAbsorption {
			return "EXIT_SHORT", true
		}
	}

	return "", false
}
