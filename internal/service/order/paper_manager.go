package order

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/ws"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

type PaperPositionManager struct {
	mu              sync.RWMutex
	activePositions map[string]*models.Position // Key: symbol:product
	orderBook       []models.OrderBookEntry
	lastPrices      map[string]float64 // Tracks latest LTP for Market Orders
	wsHub           *ws.Hub
}

func NewPaperPositionManager(hub *ws.Hub) *PaperPositionManager {
	return &PaperPositionManager{
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		lastPrices:      make(map[string]float64),
		wsHub:           hub,
	}
}

// PlaceOrder handles the initial intent.
// MARKET orders fill immediately, LIMIT orders stay PENDING.
func (pm *PaperPositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())

	entry := models.OrderBookEntry{
		OrderID:   orderID,
		Symbol:    req.Symbol,
		Side:      req.TransactionType,
		OrderType: req.OrderType,
		Qty:       req.Quantity,
		Price:     req.Price,
		Status:    "PENDING",
		Timestamp: time.Now(),
	}

	// Immediate Execution for Market Orders
	if req.OrderType == "MARKET" {
		ltp, exists := pm.lastPrices[strings.ToUpper(req.Symbol)]
		if !exists {
			return "", fmt.Errorf("no market price available for %s", req.Symbol)
		}

		entry.Price = ltp
		entry.Status = "COMPLETE"
		entry.FilledQty = req.Quantity
		pm.updatePositionState(req, ltp)
	}

	pm.orderBook = append(pm.orderBook, entry)

	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "order_update",
			"data": entry,
		})
	}

	return orderID, nil
}

// OnPriceUpdate now checks if any PENDING limit orders should be triggered.
func (pm *PaperPositionManager) OnPriceUpdate(symbol string, ltp float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	pm.lastPrices[symbolKey] = ltp

	// 1. Check Order Book for pending LIMIT orders
	for i := range pm.orderBook {
		order := &pm.orderBook[i]

		if order.Status == "PENDING" && order.Symbol == symbol && order.OrderType == "LIMIT" {
			shouldFill := false

			// BUY LIMIT: Fill if Market Price <= Limit Price
			if order.Side == "BUY" && ltp <= order.Price {
				shouldFill = true
			}
			// SELL LIMIT: Fill if Market Price >= Limit Price
			if order.Side == "SELL" && ltp >= order.Price {
				shouldFill = true
			}

			if shouldFill {
				order.Status = "COMPLETE"
				order.FilledQty = order.Qty

				// Update the Position using the Limit Price as the fill price
				req := models.OrderRequest{
					Symbol:          order.Symbol,
					Product:         "MIS", // Default for paper trading or add to OrderBookEntry
					TransactionType: order.Side,
					Quantity:        order.Qty,
				}
				pm.updatePositionState(req, order.Price)

				// Notify UI of the fill
				if pm.wsHub != nil {
					pm.wsHub.BroadcastJSON("global:trading", map[string]any{
						"type": "order_update",
						"data": order,
					})
				}
			}
		}
	}

	// 2. Recalculate Unrealized PnL for active positions
	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", symbolKey, product)
		pos, exists := pm.activePositions[key]

		if exists && pos.NetQuantity != 0 {
			if pos.Side == "LONG" {
				pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
			} else {
				pos.UnrealizedPnL = (pos.AveragePrice - ltp) * float64(pos.NetQuantity)
			}

			if pm.wsHub != nil {
				payload := map[string]any{"type": "position_update", "data": pos}
				pm.wsHub.BroadcastJSON("global:trading", payload)
				pm.wsHub.BroadcastJSON(symbolKey+":1m", payload)
			}
		}
	}
}

func (pm *PaperPositionManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := pm.activePositions[key]
	return pos, exists
}

// updatePositionState handles the "Position" side of the Golden Rule.
func (pm *PaperPositionManager) updatePositionState(req models.OrderRequest, fillPrice float64) {
	key := fmt.Sprintf("%s:%s", strings.ToUpper(req.Symbol), strings.ToUpper(req.Product))
	pos, exists := pm.activePositions[key]

	if !exists {
		pos = &models.Position{
			Symbol:        req.Symbol,
			Product:       req.Product,
			TargetPrice:   req.TargetPrice,
			StopLossPrice: req.StopLossPrice,
		}
		pm.activePositions[key] = pos
	}

	// Calculate New Average Price and Net Quantity
	if req.TransactionType == "BUY" {
		totalCost := (pos.AveragePrice * float64(pos.NetQuantity)) + (fillPrice * float64(req.Quantity))
		pos.NetQuantity += req.Quantity
		pos.AveragePrice = totalCost / float64(pos.NetQuantity)

		if pos.NetQuantity > 0 {
			pos.Side = "LONG"
		} else if pos.NetQuantity < 0 {
			pos.Side = "SHORT"
		}
	} else {
		// SELL Transaction
		totalValue := (pos.AveragePrice * float64(pos.NetQuantity)) - (fillPrice * float64(req.Quantity))
		pos.NetQuantity -= req.Quantity

		if pos.NetQuantity != 0 {
			pos.AveragePrice = totalValue / float64(pos.NetQuantity)
		}

		if pos.NetQuantity > 0 {
			pos.Side = "LONG"
		} else if pos.NetQuantity < 0 {
			pos.Side = "SHORT"
		}
	}

	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "position_update",
			"data": pos,
		})
	}
}

func (pm *PaperPositionManager) GetOrders(symbol string) []models.OrderBookEntry {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var filtered []models.OrderBookEntry
	symbol = strings.ToUpper(symbol)

	for _, order := range pm.orderBook {
		if order.Symbol == symbol {
			filtered = append(filtered, order)
		}
	}
	return filtered
}

func (pm *PaperPositionManager) GetAllPositions() []models.Position {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	var list []models.Position
	for _, pos := range pm.activePositions {
		list = append(list, *pos)
	}
	return list
}
