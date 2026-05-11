package order

import (
	"fmt"
	"math"
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type PositionManager struct {
	mu              sync.RWMutex
	activePositions map[string]*models.Position
	kiteClient      *kiteconnect.Client
}

func NewPositionManager(kc *kiteconnect.Client) *PositionManager {
	return &PositionManager{
		activePositions: make(map[string]*models.Position),
		kiteClient:      kc,
	}
}

func generatePosID(symbol, product string) string {
	return fmt.Sprintf("POS-%s-%s", symbol, product)
}

// HandleOrderUpdate processes WebSocket fill messages
func (pm *PositionManager) HandleOrderUpdate(kOrder kiteconnect.Order) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	posID := generatePosID(kOrder.TradingSymbol, kOrder.Product)
	pos, exists := pm.activePositions[posID]
	if !exists {
		return // We only track positions initiated by our system for now
	}

	// Did the filled quantity change? (Handles partial fills)
	if kOrder.FilledQuantity > pos.LastFillQty {
		newShares := kOrder.FilledQuantity - pos.LastFillQty

		// Update Financials (No int casting needed anymore)
		tradeValue := newShares * kOrder.AveragePrice
		if kOrder.TransactionType == kiteconnect.TransactionTypeBuy {
			pos.NetQuantity += newShares
			pos.TotalBuyValue += tradeValue
		} else {
			pos.NetQuantity -= newShares
			pos.TotalSellValue += tradeValue
		}
		pos.LastFillQty = kOrder.FilledQuantity

		// Calculate Realized PnL if position goes flat
		if pos.NetQuantity == 0 {
			pos.RealizedPnL += pos.TotalSellValue - pos.TotalBuyValue
			pos.AveragePrice = 0
			pos.TotalBuyValue = 0
			pos.TotalSellValue = 0
		} else {
			// Calculate new average price
			if pos.NetQuantity > 0 {
				pos.AveragePrice = pos.TotalBuyValue / pos.NetQuantity
				pos.Side = "LONG"
			} else {
				pos.AveragePrice = pos.TotalSellValue / math.Abs(pos.NetQuantity)
				pos.Side = "SHORT"
			}
		}

		go pm.ReconcileRiskOrders(posID)
	}
}

// ReconcileRiskOrders syncs TP/SL orders with the current NetQuantity
func (pm *PositionManager) ReconcileRiskOrders(posID string) {
	pm.mu.RLock()
	pos, exists := pm.activePositions[posID]
	pm.mu.RUnlock()

	if !exists {
		return
	}

	absQtyFloat := math.Abs(pos.NetQuantity)
	absQtyInt := int(absQtyFloat)

	// Scenario 1: Position is closed (Target Hit / Stop Hit / Manual Exit)
	if absQtyFloat == 0 {
		logger.Infof("Position %s flat. Cancelling risk orders.", posID)
		if pos.TargetOrderID != "" {
			_, err := pm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, pos.TargetOrderID, nil)
			if err != nil {
				logger.Errorf("Failed to cancel Target Order: %v", err)
			}
		}
		if pos.StopLossOrderID != "" {
			_, err := pm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, pos.StopLossOrderID, nil)
			if err != nil {
				logger.Errorf("Failed to cancel Stop Loss Order: %v", err)
			}
		}

		pm.mu.Lock()
		pos.TargetOrderID = ""
		pos.StopLossOrderID = ""
		pm.mu.Unlock()
		return
	}

	// Scenario 2: Active Position needs Risk Orders placed or updated
	exitTxnType := kiteconnect.TransactionTypeSell
	if pos.Side == "SHORT" {
		exitTxnType = kiteconnect.TransactionTypeBuy
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// Handle Target Order
	if pos.TargetPrice > 0 {
		if pos.TargetOrderID == "" {
			// Create Target
			resp, err := pm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, kiteconnect.OrderParams{
				Exchange:        kiteconnect.ExchangeNSE,
				Tradingsymbol:   pos.Symbol,
				TransactionType: exitTxnType,
				Quantity:        absQtyInt,
				Price:           pos.TargetPrice,
				OrderType:       kiteconnect.OrderTypeLimit,
				Product:         pos.Product,
			})
			if err == nil {
				pos.TargetOrderID = resp.OrderID
			}
		} else {
			// Modify existing Target quantity
			_, err := pm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, pos.TargetOrderID, kiteconnect.OrderParams{
				Quantity: absQtyInt,
			})
			if err != nil {
				logger.Errorf("Failed to modify Target Order QTY: %v", err)
			}
		}
	}

	// Handle Stop Loss Order
	if pos.StopLossPrice > 0 {
		if pos.StopLossOrderID == "" {
			// Create SL
			resp, err := pm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, kiteconnect.OrderParams{
				Exchange:        kiteconnect.ExchangeNSE,
				Tradingsymbol:   pos.Symbol,
				TransactionType: exitTxnType,
				Quantity:        absQtyInt,
				TriggerPrice:    pos.StopLossPrice,
				OrderType:       kiteconnect.OrderTypeSLM,
				Product:         pos.Product,
			})
			if err == nil {
				pos.StopLossOrderID = resp.OrderID
			}
		} else {
			// Modify existing SL quantity
			_, err := pm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, pos.StopLossOrderID, kiteconnect.OrderParams{
				Quantity: absQtyInt,
			})
			if err != nil {
				logger.Errorf("Failed to modify SL Order QTY: %v", err)
			}
		}
	}
}

// PlaceEntryOrder handles new trades from the UI
func (pm *PositionManager) PlaceEntryOrder(req models.OrderRequest) error {
	posID := generatePosID(req.Symbol, req.Product)

	pm.mu.Lock()
	pos, exists := pm.activePositions[posID]
	if !exists {
		pos = &models.Position{
			InternalID:    posID,
			Symbol:        req.Symbol,
			Product:       req.Product,
			TargetPrice:   req.TargetPrice,
			StopLossPrice: req.StopLossPrice,
		}
		pm.activePositions[posID] = pos
	} else {
		// Update TP/SL if user is scaling in and provided new limits
		if req.TargetPrice > 0 {
			pos.TargetPrice = req.TargetPrice
		}
		if req.StopLossPrice > 0 {
			pos.StopLossPrice = req.StopLossPrice
		}
	}
	pm.mu.Unlock()

	params := kiteconnect.OrderParams{
		Exchange:        kiteconnect.ExchangeNSE,
		Tradingsymbol:   req.Symbol,
		TransactionType: req.TransactionType,
		Quantity:        int(req.Quantity),
		OrderType:       req.OrderType,
		Product:         req.Product,
		Price:           req.Price,
	}

	resp, err := pm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("Entry order failed: %v", err)
		return err
	}
	logger.Infof("Placed Entry Order: %s", resp.OrderID)
	return nil
}

func (pm *PositionManager) GetActivePositions() []models.Position {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	positions := make([]models.Position, 0, len(pm.activePositions))
	for _, pos := range pm.activePositions {
		// Dereference to return a copy of the struct, ensuring thread safety
		positions = append(positions, *pos)
	}
	return positions
}
