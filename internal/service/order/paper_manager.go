package order

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
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

	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())

	logger.Infof("Request: %+v", req)

	entry := models.OrderBookEntry{
		OrderID:   orderID,
		Symbol:    req.Symbol,
		Side:      req.TransactionType,
		Qty:       req.Quantity,
		Price:     req.Price,
		Status:    "PENDING",
		Timestamp: time.Now(),
	}

	if req.OrderType == "MARKET" {
		entry.Status = "COMPLETE"
		entry.FilledQty = req.Quantity
		pm.updatePositionState(req, req.Price)
	}

	pm.orderBook = append(pm.orderBook, entry)

	// ADD THIS: Broadcast the order update immediately
	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "order_update",
			"data": entry,
		})
	}

	return orderID, nil
}

// OnPriceUpdate is the high-frequency hook for the global stream.
// It recalculates PnL for active positions when a new price arrives.
func (pm *PaperPositionManager) OnPriceUpdate(symbol string, ltp float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Check for MIS and CNC positions for this symbol
	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
		pos, exists := pm.activePositions[key]
		for symbol, pos := range pm.activePositions {
			logger.Infof("  %s: %+v", symbol, pos)
		}

		if exists && pos.NetQuantity != 0 {
			// Calculate Unrealized PnL
			if pos.Side == "LONG" {
				pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
			} else {
				pos.UnrealizedPnL = (pos.AveragePrice - ltp) * float64(pos.NetQuantity)
			}

			// Broadcast the Position Update with live PnL
			if pm.wsHub != nil {
				pm.wsHub.BroadcastJSON("global:trading", map[string]any{
					"type": "position_update",
					"data": pos,
				})
			}
		}
	}
}

func (pm *PaperPositionManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	key := fmt.Sprintf("%s:%s", symbol, product)
	pos, exists := pm.activePositions[key]
	return pos, exists
}

// updatePositionState handles the "Position" side of the Golden Rule:
// Orders only update the Net Position.
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
