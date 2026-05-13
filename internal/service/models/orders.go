package models

import (
	"time"
)

type OrderRequest struct {
	Symbol          string  `json:"symbol"`
	Product         string  `json:"product"`          // e.g., "MIS"
	TransactionType string  `json:"transaction_type"` // "BUY" or "SELL"
	OrderType       string  `json:"order_type"`       // "MARKET" or "LIMIT"
	Quantity        int     `json:"quantity"`
	Price           float64 `json:"price,omitempty"`
	TargetPrice     float64 `json:"target_price,omitempty"`
	StopLossPrice   float64 `json:"stop_loss_price,omitempty"`
}

type OrderBookEntry struct {
	OrderID   string    `json:"order_id"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"`
	Qty       int       `json:"qty"`
	FilledQty int       `json:"filled_qty"`
	Price     float64   `json:"price"`
	Status    string    `json:"status"` // PENDING, COMPLETE, REJECTED
	Timestamp time.Time `json:"timestamp"`
}

type Position struct {
	Symbol        string  `json:"symbol"`
	Product       string  `json:"product"`
	Side          string  `json:"side"` // LONG, SHORT
	NetQuantity   int     `json:"net_quantity"`
	AveragePrice  float64 `json:"average_price"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TargetPrice   float64 `json:"target_price"`
	StopLossPrice float64 `json:"stop_loss_price"`
}
