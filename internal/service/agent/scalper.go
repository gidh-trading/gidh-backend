package agent

import (
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

type ScalperAgent struct {
	mu              sync.RWMutex
	enrichedBuffers map[string][]*models.EnrichedTick
	macroHorizons   map[string]map[string]*models.Bar
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		enrichedBuffers: make(map[string][]*models.EnrichedTick),
		macroHorizons:   make(map[string]map[string]*models.Bar),
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

	sa.mu.Lock()
	// 1. Engineering: Maintain sliding rolling history of enriched ticks
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

	// 2. Analytics: Access closed multi-timeframe horizons
	macroMap, exists := sa.macroHorizons[symbol]
	if !exists || macroMap == nil {
		sa.mu.Unlock()
		return "", false
	}
	bar1m, ok := macroMap["1m"]
	if !ok || bar1m == nil {
		sa.mu.Unlock()
		return "", false
	}
	sa.mu.Unlock()

	// 3. Strategy Analytics: Synchronized Twin Percentile Ribbon Tracking
	if positionSide == "FLAT" || positionSide == "" {

		// ⚡ Setup Check: Did the last closed 1m candle print high institutional volume?
		isHighVolumeAbnormal := bar1m.Analytics.VolumeRank >= 7

		// ⚡ Setup Check: Is the candle body length showing strong directional velocity expansion?
		isPriceStretching := bar1m.Analytics.PriceRank >= 6

		if isHighVolumeAbnormal && isPriceStretching {

			// --- EVALUATE LONG BREAKOUT SETUP ---
			isBullishConviction := bar1m.Analytics.Direction == models.DirStrongBullish || bar1m.Analytics.Direction == models.DirBullish
			if isBullishConviction {
				// Immediate Micro Check: Price is stable/rising above the breakout zone close
				if raw.LastPrice >= bar1m.Close {
					return "GO_LONG", true
				}
			}

			// --- EVALUATE SHORT BREAKDOWN SETUP ---
			isBearishConviction := bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish
			if isBearishConviction {
				// Immediate Micro Check: Price is stable/falling below the breakdown zone close
				if raw.LastPrice <= bar1m.Close {
					return "GO_SHORT", true
				}
			}
		}

	} else if positionSide == "LONG" {
		// Technical Exit for Long: Closed bar signals upside structural fatigue, wall block, or deep sell reversal
		if bar1m.Analytics.Direction == models.DirBearishAbsorption || bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish {
			return "EXIT_LONG", true
		}

	} else if positionSide == "SHORT" {
		// Technical Exit for Short: Closed bar signals downside floor absorption, or aggressive buy reversal
		if bar1m.Analytics.Direction == models.DirBullishAbsorption || bar1m.Analytics.Direction == models.DirStrongBullish || bar1m.Analytics.Direction == models.DirBullish {
			return "EXIT_SHORT", true
		}
	}

	return "", false
}
