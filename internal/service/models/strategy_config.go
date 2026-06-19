package models

import "time"

// OptimizedStrategyConfig represents a single row from the optimized_strategy_configs table
type OptimizedStrategyConfig struct {
	OptimizationDate      time.Time
	StockName             string
	EntryTF               string
	MinVolumeRank         int
	MinPriceRank          int
	MinTickRank           int
	EffScalpThreshold     float64
	DirectionMode         string
	MinEfficiencySlope    *float64 // pointer to handle potential NULL values safely
	LongTimeAboveVwapPct  *float64 // pointer to handle potential NULL values safely
	ShortTimeAboveVwapPct *float64 // pointer to handle potential NULL values safely
	TakeProfitPoints      float64
	StopLossPoints        float64
	ProfitPainRatio       float64
	SignalCount           int
}
