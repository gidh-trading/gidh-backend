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

	// 1. Thread-Safe extraction of the macro context
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

	// 2. Strategy Execution Engine using upstream enrichment rolling states directly
	switch positionSide {
	case "FLAT", "":
		// ⚡ Macro Filter: Did the last closed candle establish high institutional velocity context?
		isHighVolumeAbnormal := bar1m.Analytics.VolumeRank >= 6
		isPriceStretching := bar1m.Analytics.PriceRank >= 6

		if isHighVolumeAbnormal && isPriceStretching {
			// ⚡ Micro Trigger: Is the CURRENT incoming tick showing sustained institutional momentum?
			// Relying on the upstream enrichment stage's pre-calculated rolling calculations directly.
			isLiveVelocitySustained := enrichedTick.Enrichment.VolumeRank >= 5

			if isLiveVelocitySustained {
				// --- EVALUATE LONG BREAKOUT SETUP ---
				isBullishConviction := bar1m.Analytics.Direction == models.DirStrongBullish || bar1m.Analytics.Direction == models.DirBullish
				if isBullishConviction && raw.LastPrice >= bar1m.Close {
					return "GO_LONG", true
				}

				// --- EVALUATE SHORT BREAKDOWN SETUP ---
				isBearishConviction := bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish
				if isBearishConviction && raw.LastPrice <= bar1m.Close {
					return "GO_SHORT", true
				}
			}
		}

	case "LONG":
		// ⚡ Micro Trigger: Emergency Exit on immediate high-volume downside transaction pressure
		if enrichedTick.Enrichment.VolumeRank >= 6 && raw.LastPrice < bar1m.Close {
			return "EXIT_LONG", true
		}

		// ⚡ Macro Filter: Technical Exit on completed structural candle fatigue
		if bar1m.Analytics.Direction == models.DirBearishAbsorption ||
			bar1m.Analytics.Direction == models.DirStrongBearish ||
			bar1m.Analytics.Direction == models.DirBearish {
			return "EXIT_LONG", true
		}

	case "SHORT":
		// ⚡ Micro Trigger: Emergency Exit on immediate high-volume upside transaction pressure
		if enrichedTick.Enrichment.VolumeRank >= 6 && raw.LastPrice > bar1m.Close {
			return "EXIT_SHORT", true
		}

		// ⚡ Macro Filter: Technical Exit on completed structural candle fatigue
		if bar1m.Analytics.Direction == models.DirBullishAbsorption ||
			bar1m.Analytics.Direction == models.DirStrongBullish ||
			bar1m.Analytics.Direction == models.DirBullish {
			return "EXIT_SHORT", true
		}
	}

	return "", false
}
