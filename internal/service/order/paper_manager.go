package order

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/ws"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

type PaperPositionManager struct {
	mu              sync.RWMutex
	activePositions map[string]*models.Position // Key: symbol:product
	orderBook       []models.OrderBookEntry
	wsHub           *ws.Hub
}

func NewPaperPositionManager(hub *ws.Hub) *PaperPositionManager {
	return &PaperPositionManager{
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		wsHub:           hub,
	}
}

// PlaceOrder simulates the initial entry into a trade loop.
func (pm *PaperPositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 1. Generate a unique ID for this paper order
	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())

	// 2. Create the Order Book Entry
	entry := models.OrderBookEntry{
		OrderID:   orderID,
		Symbol:    req.Symbol,
		Side:      req.TransactionType,
		Qty:       req.Quantity,
		Price:     req.Price,
		Status:    "PENDING",
		Timestamp: time.Now(),
	}

	// 3. For Paper MARKET orders, we simulate an immediate fill at the requested price
	// In a real scenario, this would wait for a tick, but for the UI journey,
	// we want immediate feedback.
	if req.OrderType == "MARKET" {
		entry.Status = "COMPLETE"
		entry.FilledQty = req.Quantity
		pm.updatePositionState(req, req.Price)
		logger.Infof("[Paper] Market Order Filled: %s %d %s @ %.2f", req.Symbol, req.Quantity, req.TransactionType, req.Price)
	} else {
		logger.Infof("[Paper] Limit Order Placed: %s %d %s @ %.2f", req.Symbol, req.Quantity, req.TransactionType, req.Price)
	}

	pm.orderBook = append(pm.orderBook, entry)
	return orderID, nil
}

// updatePositionState handles the "Position" side of the Golden Rule:
// Orders only update the Net Position.
func (pm *PaperPositionManager) updatePositionState(req models.OrderRequest, fillPrice float64) {
	key := fmt.Sprintf("%s:%s", req.Symbol, req.Product)
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

	// Update quantities and average price
	if req.TransactionType == "BUY" {
		totalCost := (pos.AveragePrice * float64(pos.NetQuantity)) + (fillPrice * float64(req.Quantity))
		pos.NetQuantity += req.Quantity
		pos.AveragePrice = totalCost / float64(pos.NetQuantity)
		pos.Side = "LONG"
	} else {
		// Simplified for first step: assuming Sell initiates a Short or reduces Long
		totalValue := (pos.AveragePrice * float64(pos.NetQuantity)) - (fillPrice * float64(req.Quantity))
		pos.NetQuantity -= req.Quantity
		if pos.NetQuantity != 0 {
			pos.AveragePrice = totalValue / float64(pos.NetQuantity)
		}
		pos.Side = "SHORT"
	}

	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "position_update",
			"data": pos,
		})
	}

}

func (pm *PaperPositionManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", symbol, product)
	pos, exists := pm.activePositions[key]
	return pos, exists
}
