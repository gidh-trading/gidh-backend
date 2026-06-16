package models

import "time"

type PricePotential struct {
	P75 float64
	P90 float64
}

type TargetMatrix map[string]map[string]PricePotential

type StrategyTransaction struct {
	TradeID        string                 `json:"trade_id"`
	StrategyName   string                 `json:"strategy_name"`
	Instrument     string                 `json:"instrument"`
	ActionType     string                 `json:"action_type"`
	Price          float64                `json:"price"`
	Quantity       float64                `json:"quantity"`
	ExecutionTime  time.Time              `json:"execution_time"`
	TriggerReason  string                 `json:"trigger_reason"`
	CurrentPnL     float64                `json:"current_pnl"`
	PeakPnL        float64                `json:"peak_pnl"`
	MarketSnapshot map[string]interface{} `json:"market_snapshot"`
}
