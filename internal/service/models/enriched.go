package models

import "time"

type SimplifiedEnrichment struct {
	Timestamp   time.Time `json:"timestamp"`
	MinuteIndex int       `json:"minute_index"`
	VolumeRank  int       `json:"volume_rank"`
	TickRank    int       `json:"tick_rank"`
	RangeRank   int       `json:"range_rank"`
	PriceRank   int       `json:"price_rank"`
}

type EnrichedTick struct {
	Raw         TickData             `json:"raw"`
	TickVolume  int64                `json:"tick_volume"`
	MinuteIndex int                  `json:"minute_index"`
	Enrichment  SimplifiedEnrichment `json:"enrichment"`
	VolProfile  *VolumeProfileInfo   `json:"vol_profile,omitempty"`
}

// CompileObservationVector flattens real-time structural data into a clean
// 28-dimensional float32 slice matching your updated Gymnasium Observation Space sequence.
func (e *EnrichedTick) CompileObservationVector(atr14 float64, rollingBars map[string]*Bar, prevVolRank, prevTickRank int) []float32 {
	// Pre-allocate space for our expanded 28 core features
	obs := make([]float32, 0, 28)

	// ========================================================================
	// 1. Sliding 60s Buffer Ranks & Momentum Velocity (7 Dimensions)
	// ========================================================================
	obs = append(obs,
		float32(e.Enrichment.VolumeRank),
		float32(e.Enrichment.TickRank),
		float32(e.Enrichment.PriceRank),
		float32(e.Enrichment.RangeRank),
	)

	// Calculate Momentum Acceleration Deltas (t vs t-1 snapshot frames)
	volVelocity := e.Enrichment.VolumeRank - prevVolRank
	tickVelocity := e.Enrichment.TickRank - prevTickRank
	obs = append(obs, float32(volVelocity), float32(tickVelocity))

	// Aggregate Total Book Imbalance Ratio
	totalDepth := e.Raw.TotalBuyQuantity + e.Raw.TotalSellQuantity
	bookImbalance := float32(0.0)
	if totalDepth > 0 {
		bookImbalance = float32(float64(e.Raw.TotalBuyQuantity-e.Raw.TotalSellQuantity) / float64(totalDepth))
	}
	obs = append(obs, bookImbalance)

	// ========================================================================
	// 2. Multi-Timeframe Context Alignment with RangeRank (16 Dimensions)
	// ========================================================================
	timeframes := []string{"1m", "3m", "5m", "10m", "15m"}
	for _, tf := range timeframes {
		if bar, ok := rollingBars[tf]; ok && bar != nil {
			obs = append(obs,
				float32(bar.ChangePct),
				float32(bar.VolumeRank),
				float32(bar.PriceRank),
				float32(bar.RangeRank), // ⚡ Added RangeRank directly to the timeframe alignment footprints
			)
		} else {
			// Protection pad fallback if the bar history is warming up
			obs = append(obs, 0.0, 1.0, 4.0, 4.0)
		}
	}

	// ========================================================================
	// 3. Volatility-Normalized Auction Geometry & Structural Compression (5 Dimensions)
	// ========================================================================
	distToVwap := float32(0.0)
	distToPoc := float32(0.0)
	distToVah := float32(0.0)
	distToVal := float32(0.0)
	vaStretch := float32(0.0) // Consolidated market structure footprint

	if atr14 > 0 {
		distToVwap = float32((e.Raw.LastPrice - e.Raw.AverageTradedPrice) / atr14)

		if e.VolProfile != nil {
			distToPoc = float32((e.Raw.LastPrice - e.VolProfile.POC) / atr14)
			distToVah = float32((e.Raw.LastPrice - e.VolProfile.VAH) / atr14)
			distToVal = float32((e.Raw.LastPrice - e.VolProfile.VAL) / atr14)

			// ⚡ Calculate Value Area Compression awareness
			vaStretch = float32((e.VolProfile.VAH - e.VolProfile.VAL) / atr14)
		}
	}

	obs = append(obs, distToVwap, distToPoc, distToVah, distToVal, vaStretch)

	return obs
}
