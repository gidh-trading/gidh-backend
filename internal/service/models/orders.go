package models

import (
	"time"
)

type OrderRequest struct {
	Symbol          string  `json:"symbol"`
	Product         string  `json:"product"`
	TransactionType string  `json:"transaction_type"`
	OrderType       string  `json:"order_type"`
	Quantity        int     `json:"quantity"`
	Price           float64 `json:"price,omitempty"`
	TargetPrice     float64 `json:"target_price,omitempty"`
	StopLossPrice   float64 `json:"stop_loss_price,omitempty"`
	UserEmail       string  `json:"user_email,omitempty"`
}

// gidh-backend/internal/service/models/orders.go

type OrderBookEntry struct {
	OrderID       string    `json:"order_id"`
	Symbol        string    `json:"symbol"`
	Side          string    `json:"side"`
	OrderType     string    `json:"order_type"`
	Qty           int       `json:"qty"`
	FilledQty     int       `json:"filled_qty"`
	Price         float64   `json:"price"`
	Status        string    `json:"status"`
	Timestamp     time.Time `json:"timestamp"`
	TargetPrice   float64   `json:"target_price,omitempty"`
	StopLossPrice float64   `json:"stop_loss_price,omitempty"`
	UserEmail     string    `json:"user_email,omitempty"`
}

type Position struct {
	Symbol        string  `json:"symbol"`
	Product       string  `json:"product"`
	Side          string  `json:"side"`
	NetQuantity   int     `json:"net_quantity"`
	AveragePrice  float64 `json:"average_price"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TargetPrice   float64 `json:"target_price"`
	StopLossPrice float64 `json:"stop_loss_price"`

	// Live Exchange IDs
	TargetOrderID   string `json:"-"`
	StopLossOrderID string `json:"-"`
	LastFillQty     int    `json:"-"`
}
