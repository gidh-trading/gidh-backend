package order

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/db"
	"math"
	"strings"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type LiveOrderManager struct {
	mu         sync.RWMutex
	kiteClient *kiteconnect.Client
	dbWriter   *writer.DBWriter
	wsHub      *ws.Hub

	activePositions map[string]*models.Position
	orderBook       []models.OrderBookEntry

	// Tracks intent parameters (TP/SL) for placed orders while they are pending
	orderTracker map[string]*models.OrderRequest

	// Maps to quickly find sibling orders for the OCO logic (targetOrderID <-> stopLossOrderID)
	ocoLinks map[string]string
}

func NewLiveOrderManager(kc *kiteconnect.Client, hub *ws.Hub, db *writer.DBWriter) *LiveOrderManager {
	return &LiveOrderManager{
		kiteClient:      kc,
		dbWriter:        db,
		wsHub:           hub,
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		orderTracker:    make(map[string]*models.OrderRequest),
		ocoLinks:        make(map[string]string),
	}
}

// ============================================================================
// 1. Core Placement Logic
// ============================================================================

func (lm *LiveOrderManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	transactionType := kiteconnect.TransactionTypeBuy
	if strings.ToUpper(req.TransactionType) == "SELL" {
		transactionType = kiteconnect.TransactionTypeSell
	}

	orderType := kiteconnect.OrderTypeMarket
	if strings.ToUpper(req.OrderType) == "LIMIT" {
		orderType = kiteconnect.OrderTypeLimit
	}

	params := kiteconnect.OrderParams{
		Exchange:        kiteconnect.ExchangeNSE,
		Tradingsymbol:   strings.ToUpper(req.Symbol),
		TransactionType: transactionType,
		Quantity:        req.Quantity,
		Product:         kiteconnect.ProductMIS,
		OrderType:       orderType,
		Validity:        kiteconnect.ValidityDay,
	}

	if orderType == kiteconnect.OrderTypeLimit {
		params.Price = req.Price
	} else if orderType == kiteconnect.OrderTypeMarket {
		params.MarketProtection = -1 // Let Zerodha handle risk limits for Market orders
	}

	// Send to Zerodha
	orderResp, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("[Live] Failed to place order for %s: %v", req.Symbol, err)
		return "", err
	}

	// Track the order intent so we know the TP/SL when it fills
	lm.orderTracker[orderResp.OrderID] = &req

	logger.Infof("[Live] Entry Order Placed: %s for %s", orderResp.OrderID, req.Symbol)
	return orderResp.OrderID, nil
}

// HandleOrderUpdate is triggered by the WebSocket postbacks from Kite
func (lm *LiveOrderManager) HandleOrderUpdate(o kiteconnect.Order) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	logger.Infof("[Live] Order Update: %s -> %s", o.OrderID, o.Status)

	// 1. Calculate qtyDelta BEFORE updating the local order book
	fillQty := int(o.FilledQuantity)
	prevFillQty := 0
	for _, ob := range lm.orderBook {
		if ob.OrderID == o.OrderID {
			prevFillQty = ob.FilledQty
			break
		}
	}
	qtyDelta := fillQty - prevFillQty

	// 2. Map and broadcast to UI & save to DB
	entry := lm.mapKiteOrderToLocal(o)
	lm.updateLocalOrderBook(entry)

	// 3. ALWAYS update position state if there's a new fill (Fixes Exit Position bug)
	if qtyDelta > 0 {
		lm.updatePositionStateFromKite(o, qtyDelta)
	}

	// 4. Is this an ENTRY order?
	if req, isEntry := lm.orderTracker[o.OrderID]; isEntry {
		if o.Status == kiteconnect.OrderStatusComplete {
			logger.Infof("[Live] Entry Filled! Avg Price: %.2f. Placing Exit Legs.", o.AveragePrice)
			// Fire OCO Legs
			lm.placeOCOLegs(o, req)
			delete(lm.orderTracker, o.OrderID)
		} else if o.Status == kiteconnect.OrderStatusCancelled || o.Status == kiteconnect.OrderStatusRejected {
			delete(lm.orderTracker, o.OrderID)
		}
		return
	}

	// 5. Is this a Target or SL leg that just COMPLETED? (The OCO Trigger)
	if siblingID, isLeg := lm.ocoLinks[o.OrderID]; isLeg {
		if o.Status == kiteconnect.OrderStatusComplete {
			logger.Infof("[Live] Exit Leg Filled: %s. Cancelling sibling: %s", o.OrderID, siblingID)

			// Cancel the sibling leg only if one exists
			if siblingID != "" {
				_, err := lm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, siblingID, nil)
				if err != nil {
					logger.Errorf("[Live] Failed to cancel sibling OCO leg %s: %v", siblingID, err)
				}
				delete(lm.ocoLinks, siblingID)
			}

			delete(lm.ocoLinks, o.OrderID)
		} else if o.Status == kiteconnect.OrderStatusCancelled || o.Status == kiteconnect.OrderStatusRejected {
			delete(lm.ocoLinks, o.OrderID)
		}
		return
	}
}

func (lm *LiveOrderManager) placeOCOLegs(filledEntry kiteconnect.Order, req *models.OrderRequest) {
	if req.TargetPrice == 0 && req.StopLossPrice == 0 {
		return
	}

	exitSide := kiteconnect.TransactionTypeSell
	if filledEntry.TransactionType == kiteconnect.TransactionTypeSell {
		exitSide = kiteconnect.TransactionTypeBuy
	}

	filledQtyInt := int(filledEntry.FilledQuantity)
	var targetOrderID, slOrderID string

	// 1. Place Target Leg (LIMIT)
	if req.TargetPrice > 0 {
		tpParams := kiteconnect.OrderParams{
			Exchange:        kiteconnect.ExchangeNSE,
			Tradingsymbol:   filledEntry.TradingSymbol,
			TransactionType: exitSide,
			Quantity:        filledQtyInt,
			Product:         kiteconnect.ProductMIS,
			OrderType:       kiteconnect.OrderTypeLimit,
			Price:           req.TargetPrice,
			Validity:        kiteconnect.ValidityDay,
		}
		resp, tpErr := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, tpParams)
		if tpErr == nil {
			targetOrderID = resp.OrderID
		} else {
			logger.Errorf("[Live] Failed to place Target Leg: %v", tpErr)
		}
	}

	// 2. Place Stop Loss Leg (SL-M)
	if req.StopLossPrice > 0 {
		slParams := kiteconnect.OrderParams{
			Exchange:        kiteconnect.ExchangeNSE,
			Tradingsymbol:   filledEntry.TradingSymbol,
			TransactionType: exitSide,
			Quantity:        filledQtyInt,
			Product:         kiteconnect.ProductMIS,
			OrderType:       kiteconnect.OrderTypeSLM,
			TriggerPrice:    req.StopLossPrice,
			Validity:        kiteconnect.ValidityDay,
		}
		resp, slErr := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, slParams)
		if slErr == nil {
			slOrderID = resp.OrderID
		} else {
			logger.Errorf("[Live] Failed to place Stop Loss Leg: %v", slErr)
		}
	}

	// 3. Link them for OCO logic & Persist to DB
	if targetOrderID != "" || slOrderID != "" {
		if targetOrderID != "" {
			lm.ocoLinks[targetOrderID] = slOrderID
		}
		if slOrderID != "" {
			lm.ocoLinks[slOrderID] = targetOrderID
		}

		// 🧠 Save the link to DB so we can reconstruct it after restart
		pool := db.GetPool()
		if pool != nil && targetOrderID != "" && slOrderID != "" {
			ctx := context.Background()
			pool.Exec(ctx, "UPDATE gidh_orders SET sibling_order_id = $1 WHERE order_id = $2", slOrderID, targetOrderID)
			pool.Exec(ctx, "UPDATE gidh_orders SET sibling_order_id = $1 WHERE order_id = $2", targetOrderID, slOrderID)
		}
	}

	// 4. Attach exchange IDs to position state
	key := fmt.Sprintf("%s:%s", filledEntry.TradingSymbol, filledEntry.Product)
	if pos, ok := lm.activePositions[key]; ok {
		pos.TargetOrderID = targetOrderID
		pos.StopLossOrderID = slOrderID
		lm.broadcastPositionUpdate(pos)
	}
}

// ============================================================================
// 2. Modifying & Exiting (Exchange Interactions)
// ============================================================================

func (lm *LiveOrderManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 1. Identify the position
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("no active position found for %s", symbol)
	}

	// 2. Modify Target Order (if exists)
	if pos.TargetOrderID != "" && tp > 0 {
		_, err := lm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, pos.TargetOrderID, kiteconnect.OrderParams{
			OrderType: kiteconnect.OrderTypeLimit,
			Price:     tp,
		})
		if err != nil {
			logger.Errorf("[Live] Failed to modify Target: %v", err)
			return err
		}
		pos.TargetPrice = tp
	}

	// 3. Modify Stop-Loss Order (if exists)
	if pos.StopLossOrderID != "" && sl > 0 {
		_, err := lm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, pos.StopLossOrderID, kiteconnect.OrderParams{
			OrderType:    kiteconnect.OrderTypeSLM,
			TriggerPrice: sl,
		})
		if err != nil {
			logger.Errorf("[Live] Failed to modify SL: %v", err)
			return err
		}
		pos.StopLossPrice = sl
	}

	// 4. Persistence & Broadcast
	if lm.dbWriter != nil {
		lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
	}
	lm.broadcastPositionUpdate(pos)

	logger.Infof("[Live] Metadata updated for %s: TP=%.2f, SL=%.2f", symbol, tp, sl)
	return nil
}

func (lm *LiveOrderManager) ModifyOrder(orderID string, newPrice, newTP, newSL float64, userEmail string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	params := kiteconnect.OrderParams{
		OrderType: kiteconnect.OrderTypeLimit,
		Price:     newPrice,
	}

	_, err := lm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, orderID, params)
	if err != nil {
		return fmt.Errorf("failed to modify order on exchange: %v", err)
	}

	// Update intent tracker if it exists
	if req, ok := lm.orderTracker[orderID]; ok {
		req.Price = newPrice
		req.TargetPrice = newTP
		req.StopLossPrice = newSL
		req.UserEmail = userEmail
	}

	return nil
}

func (lm *LiveOrderManager) CancelOrder(orderID string, userEmail string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 1. Cancel on Exchange
	_, err := lm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, orderID, nil)
	if err != nil {
		return fmt.Errorf("failed to cancel order on exchange: %v", err)
	}

	// 2. Update the local order book state immediately so it reflects the requester
	for i, o := range lm.orderBook {
		if o.OrderID == orderID {
			lm.orderBook[i].Status = "CANCELLED"
			lm.orderBook[i].UserEmail = userEmail // 👈 Inject the requester's email here

			// Persist to DB
			if lm.dbWriter != nil {
				lm.dbWriter.PersistOrder(lm.orderBook[i])
			}
			lm.broadcastOrderUpdate(lm.orderBook[i])
			return nil
		}
	}
	return fmt.Errorf("order not found locally")
}

func (lm *LiveOrderManager) ExitPosition(ctx context.Context, symbol string, product string, quantity int, userEmail string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("no active position to exit")
	}

	side := kiteconnect.TransactionTypeSell
	if pos.Side == "SHORT" {
		side = kiteconnect.TransactionTypeBuy
	}

	// 1. Place Market Exit Order
	params := kiteconnect.OrderParams{
		Exchange:         kiteconnect.ExchangeNSE,
		Tradingsymbol:    pos.Symbol,
		TransactionType:  side,
		Quantity:         quantity,
		Product:          kiteconnect.ProductMIS,
		OrderType:        kiteconnect.OrderTypeMarket,
		MarketProtection: -1, // Adding protection here as well since it's a Market order
		Validity:         kiteconnect.ValidityDay,
	}

	resp, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		return fmt.Errorf("failed to place exit order: %v", err)
	}

	lm.orderTracker[resp.OrderID] = &models.OrderRequest{
		UserEmail: userEmail,
	}

	// 2. Cancel active OCO legs immediately
	if pos.TargetOrderID != "" {
		lm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, pos.TargetOrderID, nil)
		delete(lm.ocoLinks, pos.TargetOrderID)
		pos.TargetOrderID = ""
	}
	if pos.StopLossOrderID != "" {
		lm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, pos.StopLossOrderID, nil)
		delete(lm.ocoLinks, pos.StopLossOrderID)
		pos.StopLossOrderID = ""
	}

	return nil
}

// ============================================================================
// 3. State Management & Live Updates
// ============================================================================

// OnPriceUpdate ONLY updates Unrealized PnL (Does NOT trigger orders in live mode)
func (lm *LiveOrderManager) OnPriceUpdate(symbol string, ltp float64, ts time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", symbolKey, product)
		if pos, exists := lm.activePositions[key]; exists && pos.NetQuantity != 0 {
			pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
			lm.broadcastPositionUpdate(pos)
		}
	}
}

// updatePositionStateFromKite reconciles local position quantities using actual fill data
func (lm *LiveOrderManager) updatePositionStateFromKite(o kiteconnect.Order, qtyDelta int) {
	key := fmt.Sprintf("%s:%s", strings.ToUpper(o.TradingSymbol), strings.ToUpper(o.Product))
	pos, exists := lm.activePositions[key]

	if !exists {
		pos = &models.Position{
			Symbol:  strings.ToUpper(o.TradingSymbol),
			Product: o.Product,
		}
		lm.activePositions[key] = pos
	}

	isBuy := o.TransactionType == kiteconnect.TransactionTypeBuy
	isIncreasing := (isBuy && pos.NetQuantity >= 0) || (!isBuy && pos.NetQuantity <= 0)

	if isIncreasing {
		currentAbsQty := math.Abs(float64(pos.NetQuantity))
		totalCost := (pos.AveragePrice * currentAbsQty) + (o.AveragePrice * float64(qtyDelta))

		if isBuy {
			pos.NetQuantity += qtyDelta
		} else {
			pos.NetQuantity -= qtyDelta
		}
		pos.AveragePrice = totalCost / math.Abs(float64(pos.NetQuantity))
	} else {
		closedQty := qtyDelta
		if qtyDelta > int(math.Abs(float64(pos.NetQuantity))) {
			closedQty = int(math.Abs(float64(pos.NetQuantity)))
		}

		var tradePnL float64
		if pos.Side == "LONG" {
			tradePnL = (o.AveragePrice - pos.AveragePrice) * float64(closedQty)
		} else {
			tradePnL = (pos.AveragePrice - o.AveragePrice) * float64(closedQty)
		}
		pos.RealizedPnL += tradePnL

		if isBuy {
			pos.NetQuantity += qtyDelta
		} else {
			pos.NetQuantity -= qtyDelta
		}

		if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			pos.AveragePrice = o.AveragePrice
		} else if pos.NetQuantity == 0 {
			pos.AveragePrice = 0
			pos.UnrealizedPnL = 0
			pos.TargetOrderID = ""
			pos.StopLossOrderID = ""
		}
	}

	if pos.NetQuantity > 0 {
		pos.Side = "LONG"
	} else if pos.NetQuantity < 0 {
		pos.Side = "SHORT"
	} else {
		pos.Side = ""
	}

	if lm.dbWriter != nil {
		lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
	}
	lm.broadcastPositionUpdate(pos)
}

func (lm *LiveOrderManager) updateLocalOrderBook(entry models.OrderBookEntry) {
	updated := false
	for i, o := range lm.orderBook {
		if o.OrderID == entry.OrderID {
			lm.orderBook[i] = entry
			updated = true
			break
		}
	}
	if !updated {
		lm.orderBook = append(lm.orderBook, entry)
	}

	if lm.dbWriter != nil {
		lm.dbWriter.PersistOrder(entry)
	}
	lm.broadcastOrderUpdate(entry)
}

func (lm *LiveOrderManager) mapKiteOrderToLocal(o kiteconnect.Order) models.OrderBookEntry {
	status := o.Status

	// Map Kite string statuses directly to internal UI statuses
	if status == "OPEN" || status == "TRIGGER PENDING" {
		status = "PENDING"
	}

	req, isTracked := lm.orderTracker[o.OrderID]
	_, isBotLeg := lm.ocoLinks[o.OrderID]

	email := "bot@gidh.tech" // Fallback if traded outside Gidh
	if isTracked && req.UserEmail != "" {
		email = req.UserEmail // User Entry or Manual Exit
	} else if isBotLeg {
		email = "bot.live@gidh.tech" // System automatically placed Limit/Stop leg
	}

	entry := models.OrderBookEntry{
		OrderID:   o.OrderID,
		Symbol:    o.TradingSymbol,
		Side:      o.TransactionType,
		OrderType: o.OrderType,
		Qty:       int(o.Quantity),
		FilledQty: int(o.FilledQuantity),
		Price:     o.Price,
		Status:    status,
		Timestamp: o.OrderTimestamp.Time,
		UserEmail: email,
	}

	// Restore TP/SL intent so UI sees it while the limit order is PENDING
	if isTracked {
		entry.TargetPrice = req.TargetPrice
		entry.StopLossPrice = req.StopLossPrice
	}

	return entry
}

// ============================================================================
// 4. Interface Getters
// ============================================================================

func (lm *LiveOrderManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	return pos, exists
}

func (lm *LiveOrderManager) GetOrders(symbol string) []models.OrderBookEntry {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	var filtered []models.OrderBookEntry
	symbol = strings.ToUpper(symbol)
	for _, order := range lm.orderBook {
		if order.Symbol == symbol {
			filtered = append(filtered, order)
		}
	}
	return filtered
}

func (lm *LiveOrderManager) GetAllPositions() []models.Position {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	positions := make([]models.Position, 0, len(lm.activePositions))
	for _, pos := range lm.activePositions {
		positions = append(positions, *pos)
	}
	return positions
}

func (lm *LiveOrderManager) ClearPositions() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.activePositions = make(map[string]*models.Position)
	lm.orderBook = make([]models.OrderBookEntry, 0)
	lm.orderTracker = make(map[string]*models.OrderRequest)
	lm.ocoLinks = make(map[string]string)
	logger.Info("Live Position Manager state cleared.")
}

// --- Broadcast Helpers ---

func (lm *LiveOrderManager) broadcastOrderUpdate(entry models.OrderBookEntry) {
	if lm.wsHub != nil {
		lm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "order_update",
			"data": entry,
		})
	}
}

func (lm *LiveOrderManager) broadcastPositionUpdate(pos *models.Position) {
	if lm.wsHub != nil {
		lm.wsHub.BroadcastJSON("global:trading", map[string]any{
			"type": "position_update",
			"data": pos,
		})
	}
}

func (lm *LiveOrderManager) SyncPositions() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 1. Fetch current positions from Zerodha
	positions, err := lm.kiteClient.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to sync positions from kite: %w", err)
	}

	// 2. Rebuild local map
	newPositions := make(map[string]*models.Position)
	for _, pos := range positions.Day {
		// Only track positions with net quantity
		if pos.Quantity == 0 {
			continue
		}

		key := fmt.Sprintf("%s:%s", strings.ToUpper(pos.Tradingsymbol), strings.ToUpper(pos.Product))
		newPositions[key] = &models.Position{
			Symbol:       strings.ToUpper(pos.Tradingsymbol),
			Product:      pos.Product,
			NetQuantity:  int(pos.Quantity),
			AveragePrice: pos.AveragePrice,
			Side:         "LONG", // You can refine this based on NetQuantity sign
		}
		if pos.Quantity < 0 {
			newPositions[key].Side = "SHORT"
		}

		logger.Infof("[Sync] Loaded %s position: Qty %d", pos.Tradingsymbol, pos.Quantity)
	}

	lm.activePositions = newPositions
	return nil
}

func (lm *LiveOrderManager) SyncExchangeState(ctx context.Context) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 1. SYNC ORDERS FIRST (Critical for partial fill math)
	orders, err := lm.kiteClient.GetOrders()
	if err != nil {
		return fmt.Errorf("failed to get orders: %w", err)
	}

	pool := db.GetPool()

	for _, o := range orders {
		entry := lm.mapKiteOrderToLocal(o)
		lm.updateLocalOrderBook(entry) // This populates lm.orderBook

		// 2. RECONSTRUCT OCO LINKS FROM DB
		var siblingID string
		if pool != nil {
			err := pool.QueryRow(ctx, "SELECT sibling_order_id FROM gidh_orders WHERE order_id = $1", o.OrderID).Scan(&siblingID)
			if err == nil && siblingID != "" {
				lm.ocoLinks[o.OrderID] = siblingID
			}
		}
	}

	// 3. SYNC POSITIONS SECOND
	positions, err := lm.kiteClient.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to get positions: %w", err)
	}

	for _, pos := range positions.Day {
		if pos.Quantity == 0 {
			continue
		}

		// Logic for direction based on sign of NetQuantity
		txType := kiteconnect.TransactionTypeBuy
		if pos.Quantity < 0 {
			txType = kiteconnect.TransactionTypeSell
		}

		// Route back through the unified position reconciliation function
		lm.updatePositionStateFromKite(kiteconnect.Order{
			TradingSymbol:   pos.Tradingsymbol,
			Product:         pos.Product,
			TransactionType: txType,
			FilledQuantity:  float64(math.Abs(float64(pos.Quantity))),
			AveragePrice:    pos.AveragePrice,
		}, int(math.Abs(float64(pos.Quantity))))

		logger.Infof("[Sync] Reconstructed %s position: Qty %d", pos.Tradingsymbol, pos.Quantity)
	}

	return nil
}
