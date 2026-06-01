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
	timeframes := []string{"1m", "3m", "5m", "10m"}
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

// ObservationVector represents the distilled 22-dimensional market profile.
// This matches your Python Reinforcement Learning training array sequence.
//type ObservationVector struct {
//	// --- Sliding 60s Buffer Ranks (5 Dimensions) ---
//	VolumeRank    float32 `json:"volume_rank"`
//	TickRank      float32 `json:"tick_rank"`
//	PriceRank     float32 `json:"price_rank"`
//	RangeRank     float32 `json:"range_rank"`
//	BookImbalance float32 `json:"book_imbalance"`
//
//	// --- Multi-Timeframe Context Alignment (12 Dimensions) ---
//	TF1mChangePct  float32 `json:"tf_1m_change_pct"`
//	TF1mVolumeRank float32 `json:"tf_1m_volume_rank"`
//	TF1mPriceRank  float32 `json:"tf_1m_price_rank"`
//
//	TF3mChangePct  float32 `json:"tf_3m_change_pct"`
//	TF3mVolumeRank float32 `json:"tf_3m_volume_rank"`
//	TF3mPriceRank  float32 `json:"tf_3m_price_rank"`
//
//	TF5mChangePct  float32 `json:"tf_5m_change_pct"`
//	TF5mVolumeRank float32 `json:"tf_5m_volume_rank"`
//	TF5mPriceRank  float32 `json:"tf_5m_price_rank"`
//
//	TF10mChangePct  float32 `json:"tf_10m_change_pct"`
//	TF10mVolumeRank float32 `json:"tf_10m_volume_rank"`
//	TF10mPriceRank  float32 `json:"tf_10m_price_rank"`
//
//	// --- Volatility-Normalized Auction Geometry (5 Dimensions) ---
//	DistToVwap float32 `json:"dist_to_vwap"`
//	DistToPoc  float32 `json:"dist_to_poc"`
//	DistToVah  float32 `json:"dist_to_vah"`
//	DistToVal  float32 `json:"dist_to_val"`
//	ATR14      float32 `json:"atr_14"`
//}
