package models

import (
	"time"
)

// OrderRequest handles payload deserialization for inbound execution orders
// routed through POST /api/orders/place.
//
// Strictly handles standalone entry parameters. OCO, bracket options, or exit
// thresholds are completely omitted here as per Local Risk Separation rules.
type OrderRequest struct {
	Symbol          string  `json:"symbol"`
	Product         string  `json:"product"`          // MIS, CNC
	TransactionType string  `json:"transaction_type"` // BUY, SELL
	OrderType       string  `json:"order_type"`       // MARKET, LIMIT
	Quantity        int     `json:"quantity"`
	Price           float64 `json:"price,omitempty"`
	UserEmail       string  `json:"user_email,omitempty"`
}

// OrderBookEntry handles state tracking for live entry audit ledgers.
//
// This serves as an immutable chronological log of entry execution attempts.
// It is completely detached from trailing position metadata modifiers.
type OrderBookEntry struct {
	OrderID   string    `json:"order_id"`
	Symbol    string    `json:"symbol"`
	Side      string    `json:"side"` // BUY, SELL
	OrderType string    `json:"order_type"`
	Qty       int       `json:"qty"`
	FilledQty int       `json:"filled_qty"` // Explicitly snake_case for UI progress bar streams
	Price     float64   `json:"price"`
	Status    string    `json:"status"` // PENDING, COMPLETE, CANCELLED, REJECTED
	Timestamp time.Time `json:"timestamp"`
	UserEmail string    `json:"user_email,omitempty"`
}

// Position tracks localized RAM risk metrics and aggregate coordinates.
//
// This remains the sole source of truth for current local chart boundary
// coordinates (target_price and stop_loss_price).
type Position struct {
	Symbol        string  `json:"symbol"`
	Product       string  `json:"product"`
	Side          string  `json:"side"` // LONG, SHORT, or empty "" if flat
	NetQuantity   int     `json:"net_quantity"`
	AveragePrice  float64 `json:"average_price"`
	RealizedPnL   float64 `json:"realized_pnl"`
	UnrealizedPnL float64 `json:"unrealized_pnl"`  // Computed dynamically per tick on backend
	TargetPrice   float64 `json:"target_price"`    // Syncs visual chart target boundaries
	StopLossPrice float64 `json:"stop_loss_price"` // Syncs visual chart floor boundaries

	// Live Exchange IDs (Ignored by the decoupled front-end interface layout)
	TargetOrderID   string `json:"-"`
	StopLossOrderID string `json:"-"`
	LastFillQty     int    `json:"-"`
}
