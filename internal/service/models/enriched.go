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
// 22-dimensional float32 slice matching the Gymnasium Observation Space array sequence.
func (e *EnrichedTick) CompileObservationVector(atr14 float64, rollingBars map[string]*Bar) []float32 {
	// Pre-allocate slice space for the 22 core features
	obs := make([]float32, 0, 22)

	// ========================================================================
	// 1. Sliding 60s Buffer Ranks (Micro Triggers - 5 Dimensions)
	// ========================================================================
	obs = append(obs,
		float32(e.Enrichment.VolumeRank),
		float32(e.Enrichment.TickRank),
		float32(e.Enrichment.PriceRank),
		float32(e.Enrichment.RangeRank),
	)

	// Calculate top-of-book order book imbalance bounded strictly between [-1.0, 1.0]
	// Using correct struct names from your enriched.go layout
	totalDepth := e.Raw.TotalBuyQuantity + e.Raw.TotalSellQuantity
	bookImbalance := float32(0.0)
	if totalDepth > 0 {
		bookImbalance = float32((e.Raw.TotalBuyQuantity - e.Raw.TotalSellQuantity) / totalDepth)
	}
	obs = append(obs, bookImbalance)

	// ========================================================================
	// 2. Multi-Timeframe Context Alignment (Footprints - 12 Dimensions)
	// ========================================================================
	timeframes := []string{"1m", "3m", "5m", "10m"}
	for _, tf := range timeframes {
		if bar, ok := rollingBars[tf]; ok && bar != nil {
			obs = append(obs,
				float32(bar.ChangePct),
				float32(bar.VolumeRank),
				float32(bar.PriceRank),
			)
		} else {
			// Protection pad fallback if the bar history is warming up
			obs = append(obs, 0.0, 1.0, 4.0)
		}
	}

	// ========================================================================
	// 3. Volatility-Normalized Auction Geometry (Coordinates - 5 Dimensions)
	// ========================================================================
	distToVwap := float32(0.0)
	distToPoc := float32(0.0)
	distToVah := float32(0.0)
	distToVal := float32(0.0)

	if atr14 > 0 {
		// distToVwap doesn't require a volume profile node, calculate it immediately
		distToVwap = float32((e.Raw.LastPrice - e.Raw.AverageTradedPrice) / atr14)

		if e.VolProfile != nil {
			distToPoc = float32((e.Raw.LastPrice - e.VolProfile.POC) / atr14)
			distToVah = float32((e.Raw.LastPrice - e.VolProfile.VAH) / atr14)
			distToVal = float32((e.Raw.LastPrice - e.VolProfile.VAL) / atr14)
		}
	}

	obs = append(obs, distToVwap, distToPoc, distToVah, distToVal, float32(atr14))

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
