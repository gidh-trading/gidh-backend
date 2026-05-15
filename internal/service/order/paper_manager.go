package order

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
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

// PlaceOrder handles the initial intent. MARKET fills immediately, LIMIT stays PENDING.
func (pm *PaperPositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())

	entry := models.OrderBookEntry{
		OrderID:       orderID,
		Symbol:        req.Symbol,
		Side:          req.TransactionType,
		OrderType:     req.OrderType,
		Qty:           req.Quantity,
		Price:         req.Price,
		TargetPrice:   req.TargetPrice,
		StopLossPrice: req.StopLossPrice,
		Status:        "PENDING",
		Timestamp:     time.Now(),
	}

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
	pm.broadcastOrderUpdate(entry) // Helper used here

	return orderID, nil
}

// OnPriceUpdate checks for LIMIT fills and TP/SL triggers
func (pm *PaperPositionManager) OnPriceUpdate(symbol string, ltp float64) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	pm.lastPrices[symbolKey] = ltp

	// 1. Check Order Book for pending LIMIT orders
	for i := range pm.orderBook {
		order := &pm.orderBook[i]

		if order.Status == "PENDING" && order.Symbol == symbol && order.OrderType == "LIMIT" {
			shouldFill := (order.Side == "BUY" && ltp <= order.Price) || (order.Side == "SELL" && ltp >= order.Price)

			if shouldFill {
				order.Status = "COMPLETE"
				order.FilledQty = order.Qty

				req := models.OrderRequest{
					Symbol:          order.Symbol,
					Product:         "MIS",
					TransactionType: order.Side,
					Quantity:        order.Qty,
					TargetPrice:     order.TargetPrice,
					StopLossPrice:   order.StopLossPrice,
				}
				pm.updatePositionState(req, order.Price)
				pm.broadcastOrderUpdate(*order)
			}
		}
	}

	// 2. Recalculate PnL and Check Target/StopLoss for active positions
	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", symbolKey, product)
		pos, exists := pm.activePositions[key]

		if exists && pos.NetQuantity != 0 {

			pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)

			// Check auto-exit triggers (The Management Logic)
			isTargetHit := (pos.Side == "LONG" && pos.TargetPrice > 0 && ltp >= pos.TargetPrice) ||
				(pos.Side == "SHORT" && pos.TargetPrice > 0 && ltp <= pos.TargetPrice)

			isSLHit := (pos.Side == "LONG" && pos.StopLossPrice > 0 && ltp <= pos.StopLossPrice) ||
				(pos.Side == "SHORT" && pos.StopLossPrice > 0 && ltp >= pos.StopLossPrice)

			if isTargetHit || isSLHit {
				logger.Infof("[Paper] Exit Triggered for %s at %.2f", pos.Symbol, ltp)
				pm.executeMarketExit(pos, ltp)
			} else {
				pm.broadcastPositionUpdate(pos)
			}
		}
	}
}

// UpdatePositionMetadata updates TP/SL for active trades
func (pm *PaperPositionManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := pm.activePositions[key]
	if !exists {
		return fmt.Errorf("position not found for %s", key)
	}

	pos.TargetPrice = tp
	pos.StopLossPrice = sl
	pm.broadcastPositionUpdate(pos)
	return nil
}

// ModifyOrder updates a pending limit order price
func (pm *PaperPositionManager) ModifyOrder(orderID string, newPrice float64, newTP float64, newSL float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := range pm.orderBook {
		if pm.orderBook[i].OrderID == orderID {
			if pm.orderBook[i].Status != "PENDING" {
				return fmt.Errorf("cannot modify non-pending order")
			}
			pm.orderBook[i].Price = newPrice
			pm.orderBook[i].TargetPrice = newTP
			pm.orderBook[i].StopLossPrice = newSL
			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order %s not found", orderID)
}

// CancelOrder moves an order to CANCELLED state
func (pm *PaperPositionManager) CancelOrder(orderID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := range pm.orderBook {
		if pm.orderBook[i].OrderID == orderID {
			if pm.orderBook[i].Status != "PENDING" {
				return fmt.Errorf("order is already %s", pm.orderBook[i].Status)
			}
			pm.orderBook[i].Status = "CANCELLED"
			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order not found")
}

// ExitPosition handles partial or full manual exits
func (pm *PaperPositionManager) ExitPosition(ctx context.Context, symbol string, product string, quantity int) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := pm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("no active position to exit")
	}

	ltp := pm.lastPrices[strings.ToUpper(symbol)]
	if ltp == 0 {
		return fmt.Errorf("market price unavailable")
	}

	// Determine direction of exit
	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	pm.updatePositionState(models.OrderRequest{
		Symbol:          symbol,
		Product:         product,
		TransactionType: side,
		Quantity:        quantity,
	}, ltp)

	return nil
}

// --- Internal Helpers ---

func (pm *PaperPositionManager) updatePositionState(req models.OrderRequest, fillPrice float64) {
	key := fmt.Sprintf("%s:%s", strings.ToUpper(req.Symbol), strings.ToUpper(req.Product))
	pos, exists := pm.activePositions[key]

	// 1. Initialize or Re-initialize Risk Levels
	if !exists {
		// New position object for the session
		pos = &models.Position{
			Symbol:        req.Symbol,
			Product:       req.Product,
			TargetPrice:   req.TargetPrice,
			StopLossPrice: req.StopLossPrice,
		}
		pm.activePositions[key] = pos
	} else if pos.NetQuantity == 0 {
		// Re-opening a flat position: Apply new risk levels from the current request
		pos.TargetPrice = req.TargetPrice
		pos.StopLossPrice = req.StopLossPrice
	}

	qty := req.Quantity
	isBuy := strings.ToUpper(req.TransactionType) == "BUY"

	// Determine if we are increasing or decreasing the current risk
	// Long increase: Buy when NetQty >= 0 | Short increase: Sell when NetQty <= 0
	isIncreasing := (isBuy && pos.NetQuantity >= 0) || (!isBuy && pos.NetQuantity <= 0)

	if isIncreasing {
		// Weighted Average Price Update: Only happens when adding to the position
		currentAbsQty := math.Abs(float64(pos.NetQuantity))
		totalCost := (pos.AveragePrice * currentAbsQty) + (fillPrice * float64(qty))

		if isBuy {
			pos.NetQuantity += qty
		} else {
			pos.NetQuantity -= qty
		}

		// New average price based on the new absolute total quantity
		pos.AveragePrice = totalCost / math.Abs(float64(pos.NetQuantity))
	} else {
		// Reducing or Flipping the position: Calculate Realized PnL
		closedQty := qty
		// If the order is larger than the current position, only the current position amount is "closed"
		if qty > int(math.Abs(float64(pos.NetQuantity))) {
			closedQty = int(math.Abs(float64(pos.NetQuantity)))
		}

		var tradePnL float64
		if pos.Side == "LONG" {
			tradePnL = (fillPrice - pos.AveragePrice) * float64(closedQty)
		} else {
			// For Shorts: profit = entry - exit
			tradePnL = (pos.AveragePrice - fillPrice) * float64(closedQty)
		}
		pos.RealizedPnL += tradePnL

		// Update Net Quantity
		if isBuy {
			pos.NetQuantity += qty
		} else {
			pos.NetQuantity -= qty
		}

		// Handle Flipping: If we went from Long to Short (or vice versa), reset average price to the fill price
		if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			pos.AveragePrice = fillPrice
			// Inherit risk levels for the new flipped side if provided in request
			pos.TargetPrice = req.TargetPrice
			pos.StopLossPrice = req.StopLossPrice
		} else if pos.NetQuantity == 0 {
			// Fully Flat: Reset trade-specific metrics and clear risk levels
			pos.AveragePrice = 0
			pos.UnrealizedPnL = 0
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
		}
	}

	// 2. Update Side String
	if pos.NetQuantity > 0 {
		pos.Side = "LONG"
	} else if pos.NetQuantity < 0 {
		pos.Side = "SHORT"
	} else {
		pos.Side = ""
	}

	// 3. Broadcast the updated state to UI
	pm.broadcastPositionUpdate(pos)
}

func (pm *PaperPositionManager) executeMarketExit(pos *models.Position, price float64) {
	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	pm.updatePositionState(models.OrderRequest{
		Symbol:          pos.Symbol,
		Product:         pos.Product,
		TransactionType: side,
		Quantity:        int(math.Abs(float64(pos.NetQuantity))),
	}, price)
}

// --- Broadcast Helpers ---

func (pm *PaperPositionManager) broadcastOrderUpdate(entry models.OrderBookEntry) {
	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "order_update",
			"data": entry,
		})
	}
}

func (pm *PaperPositionManager) broadcastPositionUpdate(pos *models.Position) {
	if pm.wsHub != nil {
		payload := map[string]any{"type": "position_update", "data": pos}
		pm.wsHub.BroadcastJSON("global:trading", payload)
	}
}

// --- Existing Getters ---

func (pm *PaperPositionManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := pm.activePositions[key]
	return pos, exists
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
	positions := make([]models.Position, 0, len(pm.activePositions))
	for _, pos := range pm.activePositions {
		positions = append(positions, *pos)
	}
	return positions
}

func (pm *PaperPositionManager) ClearPositions() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.activePositions = make(map[string]*models.Position)
	pm.orderBook = make([]models.OrderBookEntry, 0)
	pm.lastPrices = make(map[string]float64)
	logger.Info("Paper Position Manager state cleared.")
}
