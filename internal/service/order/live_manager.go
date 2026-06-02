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

	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type LiveOrderManager struct {
	mu              sync.RWMutex
	kiteClient      *kiteconnect.Client
	dbWriter        *writer.DBWriter
	wsHub           *ws.Hub
	activePositions map[string]*models.Position
	orderBook       []models.OrderBookEntry
	lastPrices      map[string]float64
}

func NewLiveOrderManager(kc *kiteconnect.Client, hub *ws.Hub, db *writer.DBWriter) *LiveOrderManager {
	return &LiveOrderManager{
		kiteClient:      kc,
		dbWriter:        db,
		wsHub:           hub,
		activePositions: make(map[string]*models.Position),
		orderBook:       make([]models.OrderBookEntry, 0),
		lastPrices:      make(map[string]float64),
	}
}

// ============================================================================
// 1. Core Entry Placement Routing Layer
// ============================================================================

func (lm *LiveOrderManager) PlaceOrder(ctx context.Context, req models.OrderRequest) (string, error) {
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
		params.MarketProtection = -1 // Let Zerodha handle standard risk slippage protection bands
	}

	// Route order directly to exchange without any nested SL/TP bracket parameters attached
	orderResp, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("[Live] Failed to route order for %s: %v", req.Symbol, err)
		return "", err
	}

	logger.Infof("[Live] Entry Order Successfully Posted: %s for %s", orderResp.OrderID, req.Symbol)
	return orderResp.OrderID, nil
}

// ModifyOrder alters a pending limit entry's price boundary on the exchange
func (lm *LiveOrderManager) ModifyOrder(orderID string, newPrice float64, userEmail string) error {
	params := kiteconnect.OrderParams{
		OrderType: kiteconnect.OrderTypeLimit,
		Price:     newPrice,
	}

	_, err := lm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, orderID, params)
	if err != nil {
		return fmt.Errorf("failed to modify pending order on broker exchange: %w", err)
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == orderID {
			lm.orderBook[i].Price = newPrice
			lm.orderBook[i].UserEmail = userEmail

			if lm.dbWriter != nil {
				lm.dbWriter.PersistOrder(lm.orderBook[i])
			}
			lm.broadcastOrderUpdate(lm.orderBook[i])
			break
		}
	}

	return nil
}

// CancelOrder revokes any remaining unexecuted shares for a pending order
func (lm *LiveOrderManager) CancelOrder(orderID string, userEmail string) error {
	_, err := lm.kiteClient.CancelOrder(kiteconnect.VarietyRegular, orderID, nil)
	if err != nil {
		return fmt.Errorf("failed to cancel order on exchange: %w", err)
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == orderID {
			lm.orderBook[i].Status = "CANCELLED"
			lm.orderBook[i].UserEmail = userEmail

			if lm.dbWriter != nil {
				lm.dbWriter.PersistOrder(lm.orderBook[i])
			}
			lm.broadcastOrderUpdate(lm.orderBook[i])
			break
		}
	}

	return nil
}

// ExitPosition acts as your immediate user manual liquidation router
func (lm *LiveOrderManager) ExitPosition(ctx context.Context, symbol string, product string, quantity int, userEmail string) error {
	lm.mu.Lock()
	key := fmt.Sprintf("%s:%s", strings.ToUpper(symbol), strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		lm.mu.Unlock()
		return fmt.Errorf("no active position found to manually liquidate for %s", symbol)
	}

	side := kiteconnect.TransactionTypeSell
	if pos.Side == "SHORT" {
		side = kiteconnect.TransactionTypeBuy
	}
	lm.mu.Unlock()

	params := kiteconnect.OrderParams{
		Exchange:         kiteconnect.ExchangeNSE,
		Tradingsymbol:    strings.ToUpper(symbol),
		TransactionType:  side,
		Quantity:         quantity,
		Product:          kiteconnect.ProductMIS,
		OrderType:        kiteconnect.OrderTypeMarket,
		MarketProtection: -1,
		Validity:         kiteconnect.ValidityDay,
	}

	_, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		return fmt.Errorf("failed to place manual market exit order: %w", err)
	}

	return nil
}

// ============================================================================
// 2. Real-Time Evaluation Loop & Broker Update Channels
// ============================================================================

// OnPriceUpdate tracks open risk and scans local boundaries against the live tick stream
func (lm *LiveOrderManager) OnPriceUpdate(symbol string, ltp float64, ts time.Time) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	lm.lastPrices[symbolKey] = ltp

	for _, product := range []string{"MIS", "CNC"} {
		key := fmt.Sprintf("%s:%s", symbolKey, product)
		pos, exists := lm.activePositions[key]

		if exists && pos.NetQuantity != 0 {
			// Update ongoing open equity metrics for UI broadcasting
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
				logger.Infof("[Live] Local %s Risk Breach Detected for %s at %.2f! Executing Market Liquidation.", triggerType, pos.Symbol, ltp)

				// Automatically fire real exchange market orders matching current live net quantity
				go lm.executeBrokerMarketLiquidation(pos.Symbol, pos.Product, pos.Side, int(math.Abs(float64(pos.NetQuantity))))

				// Instantly clear boundaries in RAM to block duplicate triggers on rapid successive ticks
				pos.TargetPrice = 0
				pos.StopLossPrice = 0
			} else {
				lm.broadcastPositionUpdate(pos)
			}
		}
	}
}

// executeBrokerMarketLiquidation triggers immediate physical market liquidation on live accounts
func (lm *LiveOrderManager) executeBrokerMarketLiquidation(symbol, product, side string, quantity int) {
	exitSide := kiteconnect.TransactionTypeSell
	if side == "SHORT" {
		exitSide = kiteconnect.TransactionTypeBuy
	}

	params := kiteconnect.OrderParams{
		Exchange:         kiteconnect.ExchangeNSE,
		Tradingsymbol:    symbol,
		TransactionType:  exitSide,
		Quantity:         quantity,
		Product:          kiteconnect.ProductMIS,
		OrderType:        kiteconnect.OrderTypeMarket,
		MarketProtection: -1,
		Validity:         kiteconnect.ValidityDay,
	}

	_, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("[Live] CRITICAL: Automated Risk Liquidation Market Order failed for %s: %v", symbol, err)
	}
}

// HandleOrderUpdate ingests streaming postbacks from the Zerodha Ticker WebSocket
func (lm *LiveOrderManager) HandleOrderUpdate(o kiteconnect.Order) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	logger.Infof("[Live] Broker Order Postback Received: %s -> %s", o.OrderID, o.Status)

	fillQty := int(o.FilledQuantity)
	prevFillQty := 0
	for _, ob := range lm.orderBook {
		if ob.OrderID == o.OrderID {
			prevFillQty = ob.FilledQty
			break
		}
	}

	// Discard out-of-order network frames to prevent corrupting partial fill calculation metrics
	if fillQty < prevFillQty {
		logger.Warnf("[Live] Ignoring stale out-of-order frame update for %s (Received: %d, Current: %d)", o.OrderID, fillQty, prevFillQty)
		return
	}

	qtyDelta := fillQty - prevFillQty
	entry := lm.mapKiteOrderToLocal(o)
	lm.updateLocalOrderBookCache(entry)

	// Dynamically absorb partial executions into the active stock position box
	if qtyDelta > 0 {
		lm.updatePositionStateFromFill(o.TradingSymbol, o.Product, o.TransactionType, qtyDelta, o.AveragePrice)
	}
}

// ============================================================================
// 3. Local Memory Position Risk Mutators
// ============================================================================

func (lm *LiveOrderManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	key := fmt.Sprintf("%s:%s", symbolKey, strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("cannot assign risk boundaries to an empty position for %s", symbol)
	}

	pos.TargetPrice = tp
	pos.StopLossPrice = sl

	if lm.dbWriter != nil {
		lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
	}
	lm.broadcastPositionUpdate(pos)

	logger.Infof("[Live] In-Memory Risk Bounds Saved for %s: TP=%.2f, SL=%.2f", symbolKey, tp, sl)
	return nil
}

// updatePositionStateFromFill blends executed fills into a singular stock-level exposure container
func (lm *LiveOrderManager) updatePositionStateFromFill(symbol, product, transactionType string, qtyDelta int, averagePrice float64) {
	symbolKey := strings.ToUpper(symbol)
	key := fmt.Sprintf("%s:%s", symbolKey, strings.ToUpper(product))
	pos, exists := lm.activePositions[key]

	if !exists {
		pos = &models.Position{
			Symbol:  symbolKey,
			Product: strings.ToUpper(product),
		}
		lm.activePositions[key] = pos
	}

	isBuy := transactionType == kiteconnect.TransactionTypeBuy
	isIncreasing := (isBuy && pos.NetQuantity >= 0) || (!isBuy && pos.NetQuantity <= 0)

	if isIncreasing {
		currentAbsQty := math.Abs(float64(pos.NetQuantity))
		totalCost := (pos.AveragePrice * currentAbsQty) + (averagePrice * float64(qtyDelta))

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
			tradePnL = (averagePrice - pos.AveragePrice) * float64(closedQty)
		} else {
			tradePnL = (pos.AveragePrice - averagePrice) * float64(closedQty)
		}
		pos.RealizedPnL += tradePnL

		if isBuy {
			pos.NetQuantity += qtyDelta
		} else {
			pos.NetQuantity -= qtyDelta
		}

		// Reversal clear checking step
		if (isBuy && pos.NetQuantity > 0) || (!isBuy && pos.NetQuantity < 0) {
			pos.AveragePrice = averagePrice
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
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
		pos.TargetPrice = 0
		pos.StopLossPrice = 0
	}

	if lm.dbWriter != nil {
		lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
	}
	lm.broadcastPositionUpdate(pos)
}

// ============================================================================
// 4. Inspection Getters & Alignment Utilities
// ============================================================================

func (lm *LiveOrderManager) mapKiteOrderToLocal(o kiteconnect.Order) models.OrderBookEntry {
	status := o.Status
	if status == "OPEN" || status == "TRIGGER PENDING" || status == "UPDATE" || status == "PUT ORDER REQ RECEIVED" || status == "VALIDATION PENDING" {
		status = "PENDING"
	}

	return models.OrderBookEntry{
		OrderID:   o.OrderID,
		Symbol:    strings.ToUpper(o.TradingSymbol),
		Side:      strings.ToUpper(o.TransactionType),
		OrderType: strings.ToUpper(o.OrderType),
		Qty:       int(o.Quantity),
		FilledQty: int(o.FilledQuantity),
		Price:     o.Price,
		Status:    status,
		Timestamp: o.OrderTimestamp.Time,
		UserEmail: "bot.live@gidh.tech",
	}
}

func (lm *LiveOrderManager) updateLocalOrderBookCache(entry models.OrderBookEntry) {
	updated := false
	for i, o := range lm.orderBook {
		if o.OrderID == entry.OrderID {
			if entry.FilledQty < o.FilledQty || (o.Status == "COMPLETE" && entry.Status != "COMPLETE") {
				return // Discard retrogressive out-of-order network steps
			}
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

func (lm *LiveOrderManager) SyncExchangeState(ctx context.Context) error {
	orders, err := lm.kiteClient.GetOrders()
	if err != nil {
		return fmt.Errorf("failed to recover order book lines from exchange: %w", err)
	}

	lm.mu.Lock()
	for _, o := range orders {
		entry := lm.mapKiteOrderToLocal(o)
		lm.updateLocalOrderBookCache(entry)
	}
	lm.mu.Unlock()

	positions, err := lm.kiteClient.GetPositions()
	if err != nil {
		return fmt.Errorf("failed to recover position frames from exchange: %w", err)
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	for _, pos := range positions.Net {
		if pos.Quantity == 0 {
			continue
		}

		txType := kiteconnect.TransactionTypeBuy
		if pos.Quantity < 0 {
			txType = kiteconnect.TransactionTypeSell
		}

		lm.updatePositionStateFromFill(pos.Tradingsymbol, pos.Product, txType, int(math.Abs(float64(pos.Quantity))), pos.AveragePrice)
		logger.Infof("[Sync] Successfully recovered live active position tracking: %s Qty %d", pos.Tradingsymbol, pos.Quantity)
	}

	return nil
}

func (lm *LiveOrderManager) broadcastOrderUpdate(entry models.OrderBookEntry) {
	if lm.wsHub != nil {
		lm.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "order_update", "data": entry})
	}
}

func (lm *LiveOrderManager) broadcastPositionUpdate(pos *models.Position) {
	if lm.wsHub != nil {
		lm.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "position_update", "data": pos})
	}
}

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
	lm.lastPrices = make(map[string]float64)
	logger.Info("Live Position Manager cache state wiped cleanly.")
}
