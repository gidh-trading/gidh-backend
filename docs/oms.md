# Technical Specification: Position-Based Order Management System (OMS)

## 1. Executive Summary

This document outlines the architectural transition from a simple 1-to-1 order tracking system to a **Position-Based OMS** for the `gidh-backend`.

To robustly support intraday trading features like Stop Loss (SL), Take Profit (TP), Scaling In (Averaging), and Partial Fills, the backend must decouple "Entry Orders" from "Exit Orders". The new architecture relies on the **Golden Rule:** *Entry orders only update the net Position. The net Position state dictates the size and existence of the TP/SL orders.*

## 2. Core Concepts

* **Order Book**: Tracks the raw intent sent to Zerodha (Pending, Complete, Cancelled).
* **Position Book**: The single source of truth for current holdings per `Symbol` + `Product` (e.g., SBIN-MIS).
* **Risk Orders (Exits)**: Every active Position will have a maximum of *one* live Target order and *one* live Stop Loss order on the exchange at any given time.

---

## 3. Domain Model Updates

Update `internal/service/models/domain.go` to include the `Position` entity. This struct must be tracked in-memory by the Position Manager.

```go
// internal/service/models/domain.go

// OrderRequest represents the payload from the UI
type OrderRequest struct {
    Symbol          string  `json:"symbol"`
    Product         string  `json:"product"`          // e.g., "MIS"
    TransactionType string  `json:"transaction_type"` // "BUY" or "SELL"
    OrderType       string  `json:"order_type"`       // "MARKET" or "LIMIT"
    Quantity        int     `json:"quantity"`
    Price           float64 `json:"price,omitempty"`  // For LIMIT entry
    
    // Risk Parameters
    TargetPrice     float64 `json:"target_price,omitempty"`
    StopLossPrice   float64 `json:"stop_loss_price,omitempty"`
}

// Position tracks the live, aggregated state of a specific symbol
type Position struct {
    InternalID      string  `json:"internal_id"` // e.g., "POS-SBIN-MIS-BUY"
    Symbol          string  `json:"symbol"`
    Product         string  `json:"product"`
    Side            string  `json:"side"`        // "LONG" or "SHORT"
    
    // Core State
    NetQuantity     int     `json:"net_quantity"`
    AveragePrice    float64 `json:"average_price"` 
    LastFillQty     int     `json:"-"` // Tracks previous WS update fill delta
    
    // Profit and Loss
    RealizedPnL     float64 `json:"realized_pnl"`
    TotalBuyValue   float64 `json:"total_buy_value"`
    TotalSellValue  float64 `json:"total_sell_value"`

    // Live Risk State
    TargetPrice     float64 `json:"target_price"`
    StopLossPrice   float64 `json:"stop_loss_price"`
    TargetOrderID   string  `json:"target_order_id"`    // Live Kite Order ID
    StopLossOrderID string  `json:"stop_loss_order_id"` // Live Kite Order ID
}

```

---

## 4. Architecture: `PositionManager`

Create a new service in `internal/service/order/manager.go`. This service will hold the in-memory map of active positions and expose methods for the HTTP handlers and the WebSocket listener.

```go
package order

import (
    "sync"
    "gidh-backend/internal/service/models"
    kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type PositionManager struct {
    mu              sync.RWMutex
    activePositions map[string]*models.Position // Keyed by InternalID
    kiteClient      *kiteconnect.Client
}

// ... Initialization and methods ...

```

---

## 5. Primary Workflows (State Machine)

### Workflow A: New Trade Entry (HTTP)

1. **Endpoint:** `POST /api/orders/place`
2. **Logic:**
* Create a new `Position` in the map with `NetQuantity = 0` (if it doesn't already exist for the day).
* Save the requested `TargetPrice` and `StopLossPrice` in the Position.
* Send the Entry Order (`MARKET` or `LIMIT`) to Kite Connect.
* **Do not** send TP/SL orders to Kite yet.



### Workflow B: The Fill Handler (WebSocket)

In `internal/service/stream/live_source.go`, intercept `OnOrderUpdate` and pass it to the `PositionManager`. This is where all logic evaluates.

```go
func (pm *PositionManager) HandleOrderUpdate(kOrder kiteconnect.Order) {
    pm.mu.Lock()
    defer pm.mu.Unlock()

    posID := generatePosID(kOrder.Tradingsymbol, kOrder.Product)
    pos, exists := pm.activePositions[posID]
    if !exists { return }

    // Did the filled quantity change? (Handles partial fills seamlessly)
    if kOrder.FilledQuantity > pos.LastFillQty {
        newShares := kOrder.FilledQuantity - pos.LastFillQty
        
        // 1. Update Financials
        tradeValue := float64(newShares) * kOrder.AveragePrice
        if kOrder.TransactionType == "BUY" {
            pos.NetQuantity += newShares
            pos.TotalBuyValue += tradeValue
        } else {
            pos.NetQuantity -= newShares
            pos.TotalSellValue += tradeValue
        }
        pos.LastFillQty = kOrder.FilledQuantity

        // Calculate Realized PnL if position goes flat
        if pos.NetQuantity == 0 {
            pos.RealizedPnL = pos.TotalSellValue - pos.TotalBuyValue
            pos.AveragePrice = 0
        }

        // 2. Trigger Risk Reconciliation (Run in a separate goroutine to avoid blocking WS)
        go pm.ReconcileRiskOrders(posID)
    }
}

```

### Workflow C: Risk Reconciliation (`ReconcileRiskOrders`)

This function fires every time `NetQuantity` changes. It compares the current `NetQuantity` against the live TP/SL orders on Zerodha.

* **If `NetQuantity > 0` but NO Risk Orders exist:** * Send `POST` requests to create Target (`LIMIT`) and Stop Loss (`SL-M`) orders for exactly `NetQuantity` shares.
* Save the new Kite `order_id`s to `TargetOrderID` and `StopLossOrderID`.


* **If `NetQuantity` > active Risk Order Quantity (Scaling In):**
* Send `PUT` requests to Kite modifying `TargetOrderID` and `StopLossOrderID` so `quantity = NetQuantity`.


* **If `NetQuantity == 0` (Target Hit / Exit):**
* **OCO Logic Triggered:** Send `DELETE` requests to Kite for any pending `TargetOrderID` or `StopLossOrderID`.



### Workflow D: Manual Order Cancellation / Exit

If the user clicks "Exit Position" from the UI:

1. Lock the `Position`.
2. Cancel both `TargetOrderID` and `StopLossOrderID` via HTTP `DELETE` to Kite.
3. Fire a `MARKET` order in the opposite direction for the current `NetQuantity` to square off the trade.

---

## 6. Realized vs. Unrealized PnL

The frontend requires two metrics:

1. **Realized PnL:** Locked-in profit from closed loops.
* *Formula:* `Position.TotalSellValue - Position.TotalBuyValue` (Calculated only when `NetQuantity == 0`).


2. **Unrealized PnL:** Live floating profit.
* *Formula (LONG):* `(CurrentLTP * NetQuantity) - (AveragePrice * NetQuantity)`
* *Formula (SHORT):* `(AveragePrice * NetQuantity) - (CurrentLTP * NetQuantity)`



---

## 7. Crucial Engineering Guidelines

1. **Thread Safety:** The `PositionManager` will be accessed by both the HTTP server (user adding quantity, modifying limits) and the WebSocket connection (fill updates) simultaneously. Strict use of `sync.RWMutex` around map accesses is mandatory. Do not make HTTP calls to Zerodha while holding the lock.
2. **Kite Order Types:**
* Target orders must be `regular` variety, `LIMIT` type.
* Stop Loss orders should ideally be `regular` variety, `SL-M` type (using `trigger_price`).


3. **Background Reconciliation (CRON):** Because WebSockets can drop packets or servers can restart, implement a 5-minute background ticker that calls `GET /portfolio/positions` on Kite. It should compare Zerodha's absolute `NetQuantity` against the internal Go map's `NetQuantity` and correct any drift.