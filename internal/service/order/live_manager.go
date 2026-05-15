package order

import (
	"context"
	"fmt"
	"gidh-backend/pkg/logger"
	"math"
	"strings"
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/ws"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type LivePositionManager struct {
	mu              sync.RWMutex
	activePositions map[string]*models.Position      // Key: symbol:product
	orderBook       map[string]models.OrderBookEntry // Internal cache of live orders
	lastPrices      map[string]float64
	kite            *kiteconnect.Client
	wsHub           *ws.Hub
}

func NewLivePositionManager(client *kiteconnect.Client, hub *ws.Hub) *LivePositionManager {
	return &LivePositionManager{
		activePositions: make(map[string]*models.Position),
		orderBook:       make(map[string]models.OrderBookEntry),
		lastPrices:      make(map[string]float64),
		kite:            client,
		wsHub:           hub,
	}
}

func (lm *LivePositionManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
	// 1. Prepare Kite Order Parameters
	params := kiteconnect.OrderParams{
		Exchange:         kiteconnect.ExchangeNSE,
		Tradingsymbol:    req.Symbol,
		TransactionType:  req.TransactionType,
		Quantity:         req.Quantity,
		Price:            req.Price,
		OrderType:        req.OrderType,
		Product:          req.Product,
		Validity:         kiteconnect.ValidityDay,
		MarketProtection: -1, // As requested for volatility protection
	}

	// 2. Execute via Zerodha API
	orderResponse, err := lm.kite.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("Kite PlaceOrder Error: %v", err)
		return "", err
	}

	// 3. Initialize the Position entry if it doesn't exist
	// This stores your desired TP/SL targets before the fill actually happens.
	lm.mu.Lock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(req.Symbol), strings.ToUpper(req.Product))
	if _, exists := lm.activePositions[key]; !exists {
		lm.activePositions[key] = &models.Position{
			Symbol:        req.Symbol,
			Product:       req.Product,
			TargetPrice:   req.TargetPrice,
			StopLossPrice: req.StopLossPrice,
		}
	}
	lm.mu.Unlock()

	return orderResponse.OrderID, nil
}

func (lm *LivePositionManager) HandleOrderUpdate(kOrder kiteconnect.Order) {
	// Only act on successful fills
	if kOrder.Status != "COMPLETE" {
		return
	}

	lm.mu.Lock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(kOrder.TradingSymbol), strings.ToUpper(kOrder.Product))
	pos, exists := lm.activePositions[key]
	if !exists {
		lm.mu.Unlock()
		return
	}

	// 1. Calculate the fill delta (handles partial fills correctly)
	newShares := int(kOrder.FilledQuantity) - pos.LastFillQty
	if newShares <= 0 {
		lm.mu.Unlock()
		return
	}

	isBuy := strings.ToUpper(kOrder.TransactionType) == "BUY"
	tradeValue := float64(newShares) * kOrder.AveragePrice

	// 2. Update Position Financials
	// Logic matches the "Weighted Average Price" rule from your OMS spec
	isIncreasing := (isBuy && pos.NetQuantity >= 0) || (!isBuy && pos.NetQuantity <= 0)

	if isIncreasing {
		currentAbsQty := math.Abs(float64(pos.NetQuantity))
		totalCost := (pos.AveragePrice * currentAbsQty) + tradeValue

		if isBuy {
			pos.NetQuantity += newShares
		} else {
			pos.NetQuantity -= newShares
		}
		pos.AveragePrice = totalCost / math.Abs(float64(pos.NetQuantity))
	} else {
		// Reducing or Flipping: Calculate Realized PnL
		closedQty := int(math.Min(float64(newShares), math.Abs(float64(pos.NetQuantity))))
		var tradePnL float64
		if pos.NetQuantity > 0 {
			tradePnL = (kOrder.AveragePrice - pos.AveragePrice) * float64(closedQty)
		} else {
			tradePnL = (pos.AveragePrice - kOrder.AveragePrice) * float64(closedQty)
		}
		pos.RealizedPnL += tradePnL

		if isBuy {
			pos.NetQuantity += newShares
		} else {
			pos.NetQuantity -= newShares
		}

		if pos.NetQuantity == 0 {
			pos.AveragePrice = 0
		} else if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			// Handle Flip side
			pos.AveragePrice = kOrder.AveragePrice
		}
	}

	pos.LastFillQty = int(kOrder.FilledQuantity)

	// Update Side string
	if pos.NetQuantity > 0 {
		pos.Side = "LONG"
	} else if pos.NetQuantity < 0 {
		pos.Side = "SHORT"
	} else {
		pos.Side = ""
	}
	lm.mu.Unlock()

	// 3. Trigger Risk Reconciliation (Manage TP/SL on Exchange)
	go lm.reconcileRiskOrders(key)

	// 4. Notify UI
	lm.broadcastPositionUpdate(pos)
}

func (lm *LivePositionManager) OnPriceUpdate(symbol string, ltp float64) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	lm.lastPrices[symbolKey] = ltp

	// Update PnL for all products (MIS/CNC) associated with this symbol
	for key, pos := range lm.activePositions {
		if strings.HasPrefix(key, symbolKey+":") && pos.NetQuantity != 0 {
			if pos.Side == "LONG" {
				pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
			} else {
				// For Shorts: (Entry - Exit) * Qty
				pos.UnrealizedPnL = (pos.AveragePrice - ltp) * math.Abs(float64(pos.NetQuantity))
			}
			// Notify UI of the new floating PnL
			lm.broadcastPositionUpdate(pos)
		}
	}
}

func (lm *LivePositionManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	lm.mu.Lock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists {
		lm.mu.Unlock()
		return fmt.Errorf("position not found for %s", key)
	}

	pos.TargetPrice = tp
	pos.StopLossPrice = sl
	lm.mu.Unlock()

	// Trigger reconciliation to modify the real orders on Kite
	go lm.reconcileRiskOrders(key)

	lm.broadcastPositionUpdate(pos)
	return nil
}

func (lm *LivePositionManager) ModifyOrder(orderID string, newPrice float64, newTP float64, newSL float64) error {
	// In Live mode, this calls the Kite ModifyOrder API
	params := kiteconnect.OrderParams{
		Price:     newPrice,
		OrderType: kiteconnect.OrderTypeLimit,
	}
	_, err := lm.kite.ModifyOrder(kiteconnect.VarietyRegular, orderID, params)
	return err
}

func (lm *LivePositionManager) CancelOrder(orderID string) error {
	_, err := lm.kite.CancelOrder(kiteconnect.VarietyRegular, orderID, nil)
	return err
}

func (lm *LivePositionManager) ExitPosition(ctx context.Context, symbol string, product string, quantity int) error {
	lm.mu.Lock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		lm.mu.Unlock()
		return fmt.Errorf("no active position to exit for %s", symbol)
	}

	// 1. Capture and clear risk order IDs to prevent race conditions during reconciliation
	targetID := pos.TargetOrderID
	slID := pos.StopLossOrderID
	pos.TargetOrderID = ""
	pos.StopLossOrderID = ""
	lm.mu.Unlock()

	// 2. Cancel active TP/SL orders on Kite
	if targetID != "" {
		lm.kite.CancelOrder(kiteconnect.VarietyRegular, targetID, nil)
	}
	if slID != "" {
		lm.kite.CancelOrder(kiteconnect.VarietyRegular, slID, nil)
	}

	// 3. Place Market Exit Order
	exitType := kiteconnect.TransactionTypeSell
	if pos.Side == "SHORT" {
		exitType = kiteconnect.TransactionTypeBuy
	}

	_, err := lm.kite.PlaceOrder(kiteconnect.VarietyRegular, kiteconnect.OrderParams{
		Exchange:         kiteconnect.ExchangeNSE,
		Tradingsymbol:    symbol,
		TransactionType:  exitType,
		Quantity:         quantity,
		OrderType:        kiteconnect.OrderTypeMarket,
		Product:          product,
		MarketProtection: -1,
	})

	return err
}

func (lm *LivePositionManager) GetPosition(symbol string, product string) (*models.Position, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	return pos, exists
}

func (lm *LivePositionManager) GetAllPositions() []models.Position {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	positions := make([]models.Position, 0, len(lm.activePositions))
	for _, pos := range lm.activePositions {
		positions = append(positions, *pos)
	}
	return positions
}

func (lm *LivePositionManager) ClearPositions() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	// NOTE: In Live mode, this only clears local memory.
	// Actual exchange orders should be handled via ExitPosition or manual Kite cleanup.
	lm.activePositions = make(map[string]*models.Position)
	lm.orderBook = make(map[string]models.OrderBookEntry)
	lm.lastPrices = make(map[string]float64)
	logger.Info("Live Position Manager local state cleared.")
}

// GetOrders can be implemented by fetching directly from Kite for the most accurate state
func (lm *LivePositionManager) GetOrders(symbol string) []models.OrderBookEntry {
	orders, err := lm.kite.GetOrders()
	if err != nil {
		logger.Errorf("Failed to fetch orders from Kite: %v", err)
		return nil
	}

	var filtered []models.OrderBookEntry
	symbol = strings.ToUpper(symbol)
	for _, o := range orders {
		if o.TradingSymbol == symbol {
			filtered = append(filtered, models.OrderBookEntry{
				OrderID:   o.OrderID,
				Symbol:    o.TradingSymbol,
				Side:      o.TransactionType,
				OrderType: o.OrderType,
				Qty:       int(o.Quantity),
				FilledQty: int(o.FilledQuantity),
				Price:     o.Price,
				Status:    o.Status,
				Timestamp: o.OrderTimestamp.Time,
			})
		}
	}
	return filtered
}

func (lm *LivePositionManager) broadcastPositionUpdate(pos *models.Position) {
	if lm.wsHub == nil {
		return
	}

	payload := map[string]any{
		"type": "position_update",
		"data": pos,
	}

	// Broadcast to the Global Trading Feed (Order Book/Position Table)
	lm.wsHub.BroadcastJSON("global:trading", payload)

	logger.Debugf("Broadcasted position update for %s (Qty: %d, PnL: %.2f)",
		pos.Symbol, pos.NetQuantity, pos.UnrealizedPnL)
}

func (lm *LivePositionManager) reconcileRiskOrders(key string) {
	lm.mu.Lock()
	pos, exists := lm.activePositions[key]
	if !exists {
		lm.mu.Unlock()
		return
	}

	// Capture state to release lock before making network calls
	absQty := int(math.Abs(float64(pos.NetQuantity)))
	side := pos.Side
	targetPrice := pos.TargetPrice
	slPrice := pos.StopLossPrice
	targetOrderID := pos.TargetOrderID
	slOrderID := pos.StopLossOrderID
	symbol := pos.Symbol
	product := pos.Product
	lm.mu.Unlock()

	// 1. If Position is Flat: Cancel all pending risk orders (OCO Logic)
	if absQty == 0 {
		if targetOrderID != "" {
			_, err := lm.kite.CancelOrder(kiteconnect.VarietyRegular, targetOrderID, nil)
			if err == nil {
				lm.updateTargetOrderID(key, "") // Helper to clear ID locally
			}
		}
		if slOrderID != "" {
			_, err := lm.kite.CancelOrder(kiteconnect.VarietyRegular, slOrderID, nil)
			if err == nil {
				lm.updateStopLossOrderID(key, "")
			}
		}
		return
	}

	// 2. Determine Exit Side (Opposite of Current Position)
	exitSide := kiteconnect.TransactionTypeSell
	if side == "SHORT" {
		exitSide = kiteconnect.TransactionTypeBuy
	}

	// 3. Reconcile Target (LIMIT Order)
	if targetPrice > 0 {
		if targetOrderID == "" {
			// Place New Target
			resp, err := lm.kite.PlaceOrder(kiteconnect.VarietyRegular, kiteconnect.OrderParams{
				Exchange:        kiteconnect.ExchangeNSE,
				Tradingsymbol:   symbol,
				TransactionType: exitSide,
				Quantity:        absQty,
				OrderType:       kiteconnect.OrderTypeLimit,
				Price:           targetPrice,
				Product:         product,
			})
			if err == nil {
				lm.updateTargetOrderID(key, resp.OrderID)
			} else {
				logger.Errorf("Failed to place Target for %s: %v", symbol, err)
			}
		} else {
			// Modify Existing Target Quantity/Price
			_, err := lm.kite.ModifyOrder(kiteconnect.VarietyRegular, targetOrderID, kiteconnect.OrderParams{
				Quantity:  absQty,
				Price:     targetPrice,
				OrderType: kiteconnect.OrderTypeLimit,
			})
			if err != nil {
				logger.Warnf("Target Modification Failed for %s (Order may be closed): %v", symbol, err)
			}
		}
	}

	// 4. Reconcile Stop Loss (SL-M Order)
	if slPrice > 0 {
		if slOrderID == "" {
			// Place New Stop Loss
			resp, err := lm.kite.PlaceOrder(kiteconnect.VarietyRegular, kiteconnect.OrderParams{
				Exchange:        kiteconnect.ExchangeNSE,
				Tradingsymbol:   symbol,
				TransactionType: exitSide,
				Quantity:        absQty,
				OrderType:       kiteconnect.OrderTypeSLM,
				TriggerPrice:    slPrice,
				Product:         product,
			})
			if err == nil {
				lm.updateStopLossOrderID(key, resp.OrderID)
			} else {
				logger.Errorf("Failed to place StopLoss for %s: %v", symbol, err)
			}
		} else {
			// Modify Existing Stop Loss Quantity/Trigger
			_, err := lm.kite.ModifyOrder(kiteconnect.VarietyRegular, slOrderID, kiteconnect.OrderParams{
				Quantity:     absQty,
				TriggerPrice: slPrice,
				OrderType:    kiteconnect.OrderTypeSLM,
			})
			if err != nil {
				logger.Warnf("SL Modification Failed for %s: %v", symbol, err)
			}
		}
	}
}

// Internal thread-safe ID updaters
func (lm *LivePositionManager) updateTargetOrderID(key, id string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if pos, ok := lm.activePositions[key]; ok {
		pos.TargetOrderID = id
	}
}

func (lm *LivePositionManager) updateStopLossOrderID(key, id string) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if pos, ok := lm.activePositions[key]; ok {
		pos.StopLossOrderID = id
	}
}
