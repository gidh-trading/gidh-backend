package agent

import (
	"gidh-backend/internal/service/models"
	"sync"
)

type ScalperAgent struct {
	mu            sync.RWMutex
	macroHorizons map[string]map[string]*models.Bar

	// Strategy 1: Session State Memory
	vwapHistory  map[string][]float64 // Tracks trailing VWAP prints per instrument to parse slope
	isGapDownDay map[string]bool      // Cache to lock in asset qualification at market open
}

func NewScalperAgent() *ScalperAgent {
	return &ScalperAgent{
		macroHorizons: make(map[string]map[string]*models.Bar),
		vwapHistory:   make(map[string][]float64),
		isGapDownDay:  make(map[string]bool),
	}
}

func (sa *ScalperAgent) IngestClosedBar(bar *models.Bar) {
	sa.mu.Lock()
	defer sa.mu.Unlock()

	if sa.macroHorizons[bar.StockName] == nil {
		sa.macroHorizons[bar.StockName] = make(map[string]*models.Bar)
	}
	sa.macroHorizons[bar.StockName][bar.Timeframe] = bar

	// Append trailing 1m VWAP records to compute a rolling 3-period slope
	if bar.Timeframe == "1m" {
		sa.vwapHistory[bar.StockName] = append(sa.vwapHistory[bar.StockName], bar.VWAP)
		// Maintain a strict 4-element maximum (Current + 3 Trailing periods)
		if len(sa.vwapHistory[bar.StockName]) > 4 {
			sa.vwapHistory[bar.StockName] = sa.vwapHistory[bar.StockName][1:]
		}
	}
}

func (sa *ScalperAgent) AnalyzeMarket(enrichedTick *models.EnrichedTick, positionSide string) (string, bool) {
	raw := enrichedTick.Raw
	symbol := raw.StockName

	// ------------------------------------------------------------------------
	// LAYER 0: PRE-FLIGHT REGIME CHECK (First tick registers the session gap)
	// ------------------------------------------------------------------------
	sa.mu.Lock()
	if _, evaluated := sa.isGapDownDay[symbol]; !evaluated {
		// Use the change percentage built straight into your incoming tick stream
		// A negative change percentage on the opening print confirms a gap down
		if raw.Change < 0 {
			prevClose := raw.LastPrice - raw.Change
			if prevClose > 0 {
				gapPct := (raw.Change / prevClose) * 100
				// Qualification check: opened down more than 1% OR below yesterday's low
				if gapPct <= -1.0 || raw.Open < raw.Low {
					sa.isGapDownDay[symbol] = true
				} else {
					sa.isGapDownDay[symbol] = false
				}
			}
		} else {
			sa.isGapDownDay[symbol] = false
		}
	}

	isQualifiedGapDay := sa.isGapDownDay[symbol]
	sa.mu.Unlock()

	// If this stock did not experience a pre-market supply shock today, bypass execution entirely
	if !isQualifiedGapDay {
		return "", false
	}

	// Extract the macro 1m candle context populated by your pipeline
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

	// Fetch trailing VWAP historical array to extract the direction vector
	vHistory := sa.vwapHistory[symbol]
	sa.mu.RUnlock()

	// ------------------------------------------------------------------------
	// LAYER 1 & 2: LOCATION & REGIME TREND FILTERS
	// ------------------------------------------------------------------------
	// Price must remain strictly below the institutional volume-weighted average
	if bar1m.Close >= bar1m.VWAP {
		return "", false
	}

	// Determine institutional slope: Must have at least 2 minutes of bar history to derive slope
	if len(vHistory) < 2 {
		return "", false
	}

	// Calculate delta against the historical anchor print (up to 3 minutes lookback)
	oldestVwap := vHistory[0]
	vwapSlope := bar1m.VWAP - oldestVwap

	// If VWAP is flat or sloping upward, institutional value is rising; do not short
	if vwapSlope >= 0 {
		return "", false
	}

	// ------------------------------------------------------------------------
	// LAYER 3: EXECUTION TIMING TRIGGERS (SHORT ONLY STRATEGY)
	// ------------------------------------------------------------------------
	switch positionSide {
	case "FLAT", "":
		// ENTRY CONDITION: Extreme institutional selling urgency confirmed via ranks
		isHighVolumeBearish := bar1m.Analytics.VolumeRank >= 6 &&
			(bar1m.Analytics.Direction == models.DirStrongBearish || bar1m.Analytics.Direction == models.DirBearish)

		if isHighVolumeBearish {
			return "GO_SHORT", true
		}

	case "SHORT":
		// EXIT CONDITION: Exit if the macro footprint signals buyers are absorbing the tape
		if bar1m.Analytics.Direction == models.DirBullishAbsorption {
			return "EXIT_SHORT", true
		}
	}

	return "", false
}
