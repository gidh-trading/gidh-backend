package order

import (
	"context"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
)

type PaperPositionManager struct {
	mu              sync.RWMutex
	activePositions map[string]*models.Position
	orderBook       []models.OrderBookEntry
	lastPrices      map[string]float64
	lastTimestamps  map[string]time.Time
	currentSimTime  time.Time
	wsHub           *ws.Hub
	dbWriter        *writer.DBWriter
}

func NewPaperPositionManager(hub *ws.Hub, db *writer.DBWriter) *PaperPositionManager {
	return &PaperPositionManager{
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		lastPrices:      make(map[string]float64),
		lastTimestamps:  make(map[string]time.Time),
		wsHub:           hub,
		dbWriter:        db,
	}
}

// PlaceOrder routes entry intent. MARKET fills instantly, LIMIT remains PENDING.
func (pm *PaperPositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())
	orderTime := pm.currentSimTime
	if orderTime.IsZero() {
		orderTime = time.Now()
	}

	entry := models.OrderBookEntry{
		OrderID:   orderID,
		Symbol:    strings.ToUpper(req.Symbol),
		Side:      strings.ToUpper(req.TransactionType),
		OrderType: strings.ToUpper(req.OrderType),
		Qty:       req.Quantity,
		Price:     req.Price,
		Status:    "PENDING",
		Timestamp: orderTime,
		UserEmail: req.UserEmail,
	}

	if entry.OrderType == "MARKET" {
		ltp, exists := pm.lastPrices[entry.Symbol]
		if !exists {
			return "", fmt.Errorf("no market price available for %s", req.Symbol)
		}
		entry.Price = ltp
		entry.Status = "COMPLETE"
		entry.FilledQty = req.Quantity
		pm.updatePositionState(entry.Symbol, req.Product, entry.Side, req.Quantity, ltp, entry.Timestamp)
	}

	pm.orderBook = append(pm.orderBook, entry)
	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(entry)
	}
	pm.broadcastOrderUpdate(entry)

	return orderID, nil
}

// OnPriceUpdate parses ticks, executes limit order fills, and scans local SL/TP boundaries
func (pm *PaperPositionManager) OnPriceUpdate(symbol string, ltp float64, ts time.Time) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	pm.lastPrices[symbolKey] = ltp
	pm.lastTimestamps[symbolKey] = ts
	pm.currentSimTime = ts

	// 1. Process Pending LIMIT Orders
	for i := range pm.orderBook {
		order := &pm.orderBook[i]
		if order.Status == "PENDING" && order.OrderType == "LIMIT" && order.Symbol == symbolKey {
			shouldFill := (order.Side == "BUY" && ltp <= order.Price) || (order.Side == "SELL" && ltp >= order.Price)
			if shouldFill {
				order.Status = "COMPLETE"
				order.FilledQty = order.Qty
				order.Timestamp = ts

				if pm.dbWriter != nil {
					pm.dbWriter.PersistOrder(*order)
				}
				pm.updatePositionState(order.Symbol, "MIS", order.Side, order.Qty, order.Price, ts)
				pm.broadcastOrderUpdate(*order)
			}
		}
	}

	// 2. Track Risk Metrics & Scan for Local SL/TP Breaks
	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", symbolKey, product)
		pos, exists := pm.activePositions[key]

		if exists && pos.NetQuantity != 0 {
			// Update ongoing simulation open metrics
			pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)

			isTargetHit := (pos.Side == "LONG" && pos.TargetPrice > 0 && ltp >= pos.TargetPrice) ||
				(pos.Side == "SHORT" && pos.TargetPrice > 0 && ltp <= pos.TargetPrice)

			isSLHit := (pos.Side == "LONG" && pos.StopLossPrice > 0 && ltp <= pos.StopLossPrice) ||
				(pos.Side == "SHORT" && pos.StopLossPrice > 0 && ltp >= pos.StopLossPrice)

			if isTargetHit || isSLHit {
				triggerType := "TAKE_PROFIT"
				if isSLHit {
					triggerType = "STOP_LOSS"
				}
				logger.Infof("[Paper] Local %s Triggered for %s at %.2f", triggerType, pos.Symbol, ltp)
				pm.executeInternalMarketExit(pos, ltp, ts)
			} else {
				pm.broadcastPositionUpdate(pos)
			}
		}
	}
}

// UpdatePositionMetadata commits local boundaries straight onto our position memory box
func (pm *PaperPositionManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolUpper := strings.ToUpper(symbol)
	key := fmt.Sprintf("%s:%s", symbolUpper, strings.ToUpper(product))
	pos, exists := pm.activePositions[key]

	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("cannot assign risk limits to an empty position for %s", symbol)
	}

	// Commit floor/ceiling targets directly. 0 explicitly turns off the horizontal metric lines.
	pos.TargetPrice = tp
	pos.StopLossPrice = sl

	sessionTime := pm.lastTimestamps[symbolUpper]
	if sessionTime.IsZero() {
		sessionTime = pm.currentSimTime
	}
	if sessionTime.IsZero() {
		sessionTime = time.Now()
	}

	if pos.NetQuantity == 0 {
		pos.Side = ""
		pos.AveragePrice = 0
		pos.RealizedPnL = 0
		pos.UnrealizedPnL = 0
		pos.TargetPrice = 0
		pos.StopLossPrice = 0
	}

	if pm.dbWriter != nil {
		pm.dbWriter.PersistPositionSnapshot(pos, sessionTime)
	}

	logger.Infof("[Paper] Risk Balance Synchronized for %s: TP=%.2f, SL=%.2f", symbolUpper, tp, sl)
	pm.broadcastPositionUpdate(pos)

	return nil
}

// ModifyOrder updates a pending limit order's price within the paper book
func (pm *PaperPositionManager) ModifyOrder(orderID string, newPrice float64, userEmail string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := range pm.orderBook {
		if pm.orderBook[i].OrderID == orderID {
			if pm.orderBook[i].Status != "PENDING" {
				return fmt.Errorf("cannot modify order %s because its status is %s", orderID, pm.orderBook[i].Status)
			}

			pm.orderBook[i].Price = newPrice
			pm.orderBook[i].UserEmail = userEmail

			if pm.dbWriter != nil {
				pm.dbWriter.PersistOrder(pm.orderBook[i])
			}

			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("pending order ID %s not found in memory cache", orderID)
}

// CancelOrder revokes local tracking for pending items or unexecuted leaves of a partial fill
func (pm *PaperPositionManager) CancelOrder(orderID string, userEmail string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := range pm.orderBook {
		if pm.orderBook[i].OrderID == orderID {
			// If it's already completely filled or rejected, it can't be cancelled
			if pm.orderBook[i].Status == "COMPLETE" || pm.orderBook[i].Status == "REJECTED" || pm.orderBook[i].Status == "CANCELLED" {
				return fmt.Errorf("order is already finalized with status: %s", pm.orderBook[i].Status)
			}

			// Transition order status to CANCELLED but preserve filled_qty for progress bars
			pm.orderBook[i].Status = "CANCELLED"
			pm.orderBook[i].UserEmail = userEmail

			if pm.dbWriter != nil {
				pm.dbWriter.PersistOrder(pm.orderBook[i])
			}

			logger.Infof("[Paper] Pending entry leaves truncated for OrderID: %s (Filled: %d/%d)",
				orderID, pm.orderBook[i].FilledQty, pm.orderBook[i].Qty)

			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order ID %s not found in tracking cache", orderID)
}

// ExitPosition acts as our immediate user liquidation router
func (pm *PaperPositionManager) ExitPosition(ctx context.Context, symbol string, product string, quantity int, userEmail string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolUpper := strings.ToUpper(symbol)
	key := fmt.Sprintf("%s:%s", symbolUpper, strings.ToUpper(product))
	pos, exists := pm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("no active exposure found to liquidate for asset %s", symbol)
	}

	ltp := pm.lastPrices[symbolUpper]
	if ltp == 0 {
		return fmt.Errorf("market transaction price vector presently unavailable for %s", symbol)
	}

	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	exitTime := pm.lastTimestamps[symbolUpper]
	if exitTime.IsZero() {
		exitTime = pm.currentSimTime
	}

	exitOrderID := fmt.Sprintf("PPR-MANUAL-EXIT-%d", time.Now().UnixNano())
	exitOrder := models.OrderBookEntry{
		OrderID:   exitOrderID,
		Symbol:    symbolUpper,
		Side:      side,
		OrderType: "MARKET",
		Qty:       quantity,
		FilledQty: quantity,
		Price:     ltp,
		Status:    "COMPLETE",
		Timestamp: exitTime,
		UserEmail: userEmail,
	}

	pm.orderBook = append(pm.orderBook, exitOrder)
	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(exitOrder)
	}
	pm.broadcastOrderUpdate(exitOrder)

	pm.updatePositionState(symbolUpper, product, side, quantity, ltp, exitTime)
	return nil
}

// executeInternalMarketExit fires immediate liquidation orders matching the actual net position size
func (pm *PaperPositionManager) executeInternalMarketExit(pos *models.Position, price float64, executionTime time.Time) {
	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	// Pull the exact, live outstanding net position allocation volume
	absQty := int(math.Abs(float64(pos.NetQuantity)))

	exitOrderID := fmt.Sprintf("PPR-AUTO-RISK-EXIT-%d", executionTime.UnixNano())
	exitOrder := models.OrderBookEntry{
		OrderID:   exitOrderID,
		Symbol:    pos.Symbol,
		Side:      side,
		OrderType: "MARKET",
		Qty:       absQty,
		FilledQty: absQty,
		Price:     price,
		Status:    "COMPLETE",
		Timestamp: executionTime,
		UserEmail: "bot.risk@gidh.tech",
	}

	pm.orderBook = append(pm.orderBook, exitOrder)
	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(exitOrder)
	}
	pm.broadcastOrderUpdate(exitOrder)

	// Update our tracking maps—trailing un-filled partial entries remain entirely un-affected
	pm.updatePositionState(pos.Symbol, pos.Product, side, absQty, price, executionTime)
}

// updatePositionState calculates stock-level blended performance values
func (pm *PaperPositionManager) updatePositionState(symbol, product, side string, qty int, fillPrice float64, sessionTime time.Time) {
	key := fmt.Sprintf("%s:%s", symbol, strings.ToUpper(product))
	pos, exists := pm.activePositions[key]

	if !exists {
		pos = &models.Position{
			Symbol:  symbol,
			Product: strings.ToUpper(product),
		}
		pm.activePositions[key] = pos
	}

	isBuy := side == "BUY"
	isIncreasing := (isBuy && pos.NetQuantity >= 0) || (!isBuy && pos.NetQuantity <= 0)

	if isIncreasing {
		currentAbsQty := math.Abs(float64(pos.NetQuantity))
		totalCost := (pos.AveragePrice * currentAbsQty) + (fillPrice * float64(qty))

		if isBuy {
			pos.NetQuantity += qty
		} else {
			pos.NetQuantity -= qty
		}
		pos.AveragePrice = totalCost / math.Abs(float64(pos.NetQuantity))
	} else {
		closedQty := qty
		if qty > int(math.Abs(float64(pos.NetQuantity))) {
			closedQty = int(math.Abs(float64(pos.NetQuantity)))
		}

		var tradePnL float64
		if pos.Side == "LONG" {
			tradePnL = (fillPrice - pos.AveragePrice) * float64(closedQty)
		} else {
			tradePnL = (pos.AveragePrice - fillPrice) * float64(closedQty)
		}
		pos.RealizedPnL += tradePnL
		pos.RealizedPnL = math.Round(pos.RealizedPnL*100) / 100

		if isBuy {
			pos.NetQuantity += qty
		} else {
			pos.NetQuantity -= qty
		}

		// Reversal check step
		if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			pos.AveragePrice = fillPrice
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
		} else if pos.NetQuantity == 0 {
			pos.AveragePrice = 0
			pos.UnrealizedPnL = 0
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
		}
	}

	// Calibrate Directional Identifiers
	if pos.NetQuantity > 0 {
		pos.Side = "LONG"
	} else if pos.NetQuantity < 0 {
		pos.Side = "SHORT"
	} else {
		pos.Side = ""
		pos.TargetPrice = 0
		pos.StopLossPrice = 0
	}

	if pm.dbWriter != nil {
		pm.dbWriter.PersistPositionSnapshot(pos, sessionTime)
	}
	pm.broadcastPositionUpdate(pos)
}

// --- Internal Broadcast & Inspection Handlers ---

func (pm *PaperPositionManager) broadcastOrderUpdate(entry models.OrderBookEntry) {
	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "order_update", "data": entry})
	}
}

func (pm *PaperPositionManager) broadcastPositionUpdate(pos *models.Position) {
	if pm.wsHub != nil {
		pm.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "position_update", "data": pos})
	}
}

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
	pm.lastTimestamps = make(map[string]time.Time)
	pm.currentSimTime = time.Time{}
	logger.Info("Paper Position Manager state cleared cleanly.")
}

// ReconstituteState hydrates the active memory maps using structural records pulled from storage on startup.
func (pm *PaperPositionManager) ReconstituteState(orders []models.OrderBookEntry, positions []models.Position) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 1. Rehydrate the live engine order book cache
	pm.orderBook = orders

	// 2. Rehydrate active open exposures into RAM
	for i := range positions {
		pos := positions[i]
		// The UI only cares about tracking active boundaries for items with open market exposure
		if pos.NetQuantity != 0 {
			key := fmt.Sprintf("%s:%s", strings.ToUpper(pos.Symbol), strings.ToUpper(pos.Product))
			pm.activePositions[key] = &pos
			logger.Infof("[Paper Startup] Reconstituted active memory slot for %s: NetQty=%d, SL=%.2f, TP=%.2f",
				key, pos.NetQuantity, pos.StopLossPrice, pos.TargetPrice)
		}
	}
}
