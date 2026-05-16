package order

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/writer"
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
	activePositions map[string]*models.Position
	orderBook       []models.OrderBookEntry
	lastPrices      map[string]float64
	lastTimestamps  map[string]time.Time // NEW: Tracks latest historical timestamp per symbol
	currentSimTime  time.Time            // NEW: Tracks global simulation milestone time
	wsHub           *ws.Hub
	dbWriter        *writer.DBWriter
}

func NewPaperPositionManager(hub *ws.Hub, db *writer.DBWriter) *PaperPositionManager {
	return &PaperPositionManager{
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		lastPrices:      make(map[string]float64),
		lastTimestamps:  make(map[string]time.Time), // NEW
		wsHub:           hub,
		dbWriter:        db,
	}
}

// PlaceOrder handles the initial intent. MARKET fills immediately, LIMIT stays PENDING.
func (pm *PaperPositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	orderID := fmt.Sprintf("PPR-%d", time.Now().UnixNano())

	// FIX: Deduce simulation time context instead of system clock wall-time
	orderTime := pm.currentSimTime
	if orderTime.IsZero() {
		orderTime = time.Now()
	}

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
		Timestamp:     orderTime, // FIX
	}

	if req.OrderType == "MARKET" {
		ltp, exists := pm.lastPrices[strings.ToUpper(req.Symbol)]
		if !exists {
			return "", fmt.Errorf("no market price available for %s", req.Symbol)
		}

		entry.Price = ltp
		entry.Status = "COMPLETE"
		entry.FilledQty = req.Quantity
		pm.updatePositionState(req, ltp, entry.Timestamp)
	}

	pm.orderBook = append(pm.orderBook, entry)

	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(entry)
	}

	pm.broadcastOrderUpdate(entry)
	return orderID, nil
}

func (pm *PaperPositionManager) OnPriceUpdate(symbol string, ltp float64, ts time.Time) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	pm.lastPrices[symbolKey] = ltp
	pm.lastTimestamps[symbolKey] = ts // Track per stock timestamp
	pm.currentSimTime = ts            // Sync global execution clock

	// 1. Check Order Book for pending LIMIT orders
	for i := range pm.orderBook {
		order := &pm.orderBook[i]
		if order.Status == "PENDING" && order.Symbol == symbol && order.OrderType == "LIMIT" {
			shouldFill := (order.Side == "BUY" && ltp <= order.Price) || (order.Side == "SELL" && ltp >= order.Price)

			if shouldFill {
				order.Status = "COMPLETE"
				order.FilledQty = order.Qty
				order.Timestamp = ts // Update target order fill window to historical point

				if pm.dbWriter != nil {
					pm.dbWriter.PersistOrder(*order)
				}

				req := models.OrderRequest{
					Symbol:          order.Symbol,
					Product:         "MIS",
					TransactionType: order.Side,
					Quantity:        order.Qty,
					TargetPrice:     order.TargetPrice,
					StopLossPrice:   order.StopLossPrice,
				}
				pm.updatePositionState(req, order.Price, order.Timestamp)
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

			isTargetHit := (pos.Side == "LONG" && pos.TargetPrice > 0 && ltp >= pos.TargetPrice) ||
				(pos.Side == "SHORT" && pos.TargetPrice > 0 && ltp <= pos.TargetPrice)

			isSLHit := (pos.Side == "LONG" && pos.StopLossPrice > 0 && ltp <= pos.StopLossPrice) ||
				(pos.Side == "SHORT" && pos.StopLossPrice > 0 && ltp >= pos.StopLossPrice)

			if isTargetHit || isSLHit {
				logger.Infof("[Paper] Exit Triggered for %s at %.2f", pos.Symbol, ltp)
				pm.executeMarketExit(pos, ltp, ts) // FIX: Pass down the historical time
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

	// Persist the updated target and stop-loss rules to the DB
	sessionTime := pm.lastTimestamps[strings.ToUpper(symbol)]
	if sessionTime.IsZero() {
		sessionTime = pm.currentSimTime
	}
	if sessionTime.IsZero() {
		sessionTime = time.Now()
	}
	if pm.dbWriter != nil {
		pm.dbWriter.PersistPositionSnapshot(pos, sessionTime)
	}

	pm.broadcastPositionUpdate(pos)
	return nil
}

// ModifyOrder updates a pending limit order price and mirrors to database
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

			// FIX: Persist changes to DB
			if pm.dbWriter != nil {
				pm.dbWriter.PersistOrder(pm.orderBook[i])
			}

			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order %s not found", orderID)
}

// CancelOrder changes order state to CANCELLED and mirrors to database
func (pm *PaperPositionManager) CancelOrder(orderID string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	for i := range pm.orderBook {
		if pm.orderBook[i].OrderID == orderID {
			if pm.orderBook[i].Status != "PENDING" {
				return fmt.Errorf("order is already %s", pm.orderBook[i].Status)
			}
			pm.orderBook[i].Status = "CANCELLED"

			// FIX: Persist changes to DB
			if pm.dbWriter != nil {
				pm.dbWriter.PersistOrder(pm.orderBook[i])
			}

			pm.broadcastOrderUpdate(pm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order not found")
}

// ExitPosition handles manual exits
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

	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	// FIX: Dedure accurate backtest clock for manual exit placement logging
	exitTime := pm.lastTimestamps[strings.ToUpper(symbol)]
	if exitTime.IsZero() {
		exitTime = pm.currentSimTime
	}

	exitOrderID := fmt.Sprintf("PPR-MANUAL-EXIT-%d", time.Now().UnixNano())
	exitOrder := models.OrderBookEntry{
		OrderID:   exitOrderID,
		Symbol:    symbol,
		Side:      side,
		OrderType: "MARKET",
		Qty:       quantity,
		FilledQty: quantity,
		Price:     ltp,
		Status:    "COMPLETE",
		Timestamp: exitTime, // FIX
	}

	pm.orderBook = append(pm.orderBook, exitOrder)

	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(exitOrder)
	}
	pm.broadcastOrderUpdate(exitOrder)

	pm.updatePositionState(models.OrderRequest{
		Symbol:          symbol,
		Product:         product,
		TransactionType: side,
		Quantity:        quantity,
	}, ltp, exitTime)

	return nil
}

// Adjust updatePositionState signature to handle structural timestamp logic:
func (pm *PaperPositionManager) updatePositionState(req models.OrderRequest, fillPrice float64, sessionTime time.Time) {
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
	} else if pos.NetQuantity == 0 {
		pos.TargetPrice = req.TargetPrice
		pos.StopLossPrice = req.StopLossPrice
	}

	qty := req.Quantity
	isBuy := strings.ToUpper(req.TransactionType) == "BUY"
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

		if isBuy {
			pos.NetQuantity += qty
		} else {
			pos.NetQuantity -= qty
		}

		if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			pos.AveragePrice = fillPrice
			pos.TargetPrice = req.TargetPrice
			pos.StopLossPrice = req.StopLossPrice
		} else if pos.NetQuantity == 0 {
			pos.AveragePrice = 0
			pos.UnrealizedPnL = 0
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
		}
	}

	if pos.NetQuantity > 0 {
		pos.Side = "LONG"
	} else if pos.NetQuantity < 0 {
		pos.Side = "SHORT"
	} else {
		pos.Side = ""
	}

	// FIX: Persist position changes to DB
	if pm.dbWriter != nil {
		pm.dbWriter.PersistPositionSnapshot(pos, sessionTime)
	}

	pm.broadcastPositionUpdate(pos)
}

// executeMarketExit logs an explicit MARKET exit order into the DB historical record
func (pm *PaperPositionManager) executeMarketExit(pos *models.Position, price float64, executionTime time.Time) {
	side := "SELL"
	if pos.Side == "SHORT" {
		side = "BUY"
	}

	// Generate an exit order tracking entry for history logs
	exitOrderID := fmt.Sprintf("PPR-EXIT-%d", executionTime.UnixNano())
	exitOrder := models.OrderBookEntry{
		OrderID:   exitOrderID,
		Symbol:    pos.Symbol,
		Side:      side,
		OrderType: "MARKET",
		Qty:       int(math.Abs(float64(pos.NetQuantity))),
		FilledQty: int(math.Abs(float64(pos.NetQuantity))),
		Price:     price,
		Status:    "COMPLETE",
		Timestamp: executionTime,
	}

	pm.orderBook = append(pm.orderBook, exitOrder)

	if pm.dbWriter != nil {
		pm.dbWriter.PersistOrder(exitOrder)
	}
	pm.broadcastOrderUpdate(exitOrder)

	pm.updatePositionState(models.OrderRequest{
		Symbol:          pos.Symbol,
		Product:         pos.Product,
		TransactionType: side,
		Quantity:        int(math.Abs(float64(pos.NetQuantity))),
	}, price, executionTime)
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
	pm.lastTimestamps = make(map[string]time.Time) // NEW: Reset timestamps on teardown
	pm.currentSimTime = time.Time{}                // NEW: Reset simulation clock
	logger.Info("Paper Position Manager state cleared.")
}
