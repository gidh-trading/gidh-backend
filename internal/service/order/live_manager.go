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
		// Auto-detect whether this specific order layout needs 0.05, 0.50, or 1.00 rounding bounds
		tickSize := GetTickSizeForSymbol(req.Symbol, req.Price)
		params.Price = RoundToTick(req.Price, tickSize)
	} else if orderType == kiteconnect.OrderTypeMarket {
		params.MarketProtection = -1
	}

	// Route order directly to exchange
	orderResp, err := lm.kiteClient.PlaceOrder(kiteconnect.VarietyRegular, params)
	if err != nil {
		logger.Errorf("[Live] Failed to route order for %s: %v", req.Symbol, err)
		return "", err
	}

	lm.mu.Lock()

	// --- FIX 1: Prevent Race Conditions with WebSocket ---
	var existingIdx = -1
	for i, entry := range lm.orderBook {
		if entry.OrderID == orderResp.OrderID {
			existingIdx = i
			break
		}
	}

	if existingIdx != -1 {
		// The WebSocket beat the HTTP response. Just enrich the missing user data.
		lm.orderBook[existingIdx].UserEmail = req.UserEmail
		if lm.dbWriter != nil {
			lm.dbWriter.PersistOrder(lm.orderBook[existingIdx])
		}
	} else {
		// Normal flow: The HTTP response arrived first. Append safely.
		initialEntry := models.OrderBookEntry{
			OrderID:   orderResp.OrderID,
			Symbol:    strings.ToUpper(req.Symbol),
			Side:      string(transactionType),
			OrderType: string(orderType),
			Qty:       req.Quantity,
			Status:    "PENDING",
			Timestamp: time.Now(),
			UserEmail: req.UserEmail, // Capture requesting user email
		}
		lm.orderBook = append(lm.orderBook, initialEntry)
		if lm.dbWriter != nil {
			lm.dbWriter.PersistOrder(initialEntry)
		}
	}
	lm.mu.Unlock()

	logger.Infof("[Live] Entry Order Successfully Posted: %s for %s", orderResp.OrderID, req.Symbol)
	return orderResp.OrderID, nil
}

// ModifyOrder alters a pending limit entry's price boundary on the exchange
func (lm *LiveOrderManager) ModifyOrder(orderID string, newPrice float64, userEmail string) error {
	lm.mu.Lock()
	var symbol string
	for _, o := range lm.orderBook {
		if o.OrderID == orderID {
			symbol = o.Symbol
			break
		}
	}
	lm.mu.Unlock()

	// Apply identical adaptive logic to modifications
	tickSize := GetTickSizeForSymbol(symbol, newPrice)
	roundedPrice := RoundToTick(newPrice, tickSize)

	params := kiteconnect.OrderParams{
		OrderType: kiteconnect.OrderTypeLimit,
		Price:     roundedPrice,
	}

	_, err := lm.kiteClient.ModifyOrder(kiteconnect.VarietyRegular, orderID, params)
	if err != nil {
		return fmt.Errorf("failed to modify pending order on broker exchange: %w", err)
	}

	lm.mu.Lock()
	defer lm.mu.Unlock()

	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == orderID {
			lm.orderBook[i].Price = roundedPrice
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

			// --- FIX: Update ongoing open equity metrics dynamically based on direction ---
			if pos.Side == "LONG" {
				pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
			} else if pos.Side == "SHORT" {
				pos.UnrealizedPnL = (pos.AveragePrice - ltp) * float64(pos.NetQuantity)
			}

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
				// Note: math.Abs is completely safe here, but technically optional now since NetQuantity is always positive
				go lm.executeBrokerMarketLiquidation(pos.Symbol, pos.Product, pos.Side, pos.NetQuantity)

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

func (lm *LiveOrderManager) HandleOrderUpdate(o kiteconnect.Order) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	rawStatus := strings.ToUpper(o.Status)

	// Apply the UI Status Mapping
	mappedStatus := rawStatus
	if rawStatus == "OPEN" || rawStatus == "TRIGGER PENDING" || rawStatus == "UPDATE" || rawStatus == "PUT ORDER REQ RECEIVED" || rawStatus == "VALIDATION PENDING" {
		mappedStatus = "PENDING"
	}

	logger.Infof("[Live Engine] Processing broker update for OrderID: %s, Status: %s -> %s (Filled: %d/%d)",
		o.OrderID, rawStatus, mappedStatus, int(o.FilledQuantity), int(o.Quantity))

	var existingEntry *models.OrderBookEntry
	var previousFilledQty int

	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == o.OrderID {
			existingEntry = &lm.orderBook[i]
			break
		}
	}

	// Accurate Price Tracking Strategy
	var displayPrice float64
	if rawStatus == "COMPLETE" {
		if o.AveragePrice > 0 {
			displayPrice = o.AveragePrice
		} else {
			displayPrice = o.Price
		}
	} else {
		displayPrice = o.Price
	}

	if existingEntry == nil {
		email := ""
		for _, entry := range lm.orderBook {
			if entry.OrderID == o.OrderID && entry.UserEmail != "" {
				email = entry.UserEmail
				break
			}
		}

		if email == "" {
			email = "bot.live@gidh.tech"
		}

		newEntry := models.OrderBookEntry{
			OrderID:   o.OrderID,
			Symbol:    strings.ToUpper(o.TradingSymbol),
			Side:      strings.ToUpper(o.TransactionType),
			OrderType: strings.ToUpper(o.OrderType),
			Qty:       int(o.Quantity),
			FilledQty: int(o.FilledQuantity),
			Price:     displayPrice,
			Status:    mappedStatus,
			Timestamp: o.OrderTimestamp.Time,
			UserEmail: email,
		}
		if newEntry.Timestamp.IsZero() {
			newEntry.Timestamp = time.Now()
		}
		lm.orderBook = append(lm.orderBook, newEntry)
		existingEntry = &lm.orderBook[len(lm.orderBook)-1]
	} else {
		previousFilledQty = existingEntry.FilledQty
		existingEntry.FilledQty = int(o.FilledQuantity)
		existingEntry.Qty = int(o.Quantity)
		existingEntry.OrderType = strings.ToUpper(o.OrderType)
		existingEntry.Status = mappedStatus
		existingEntry.Price = displayPrice
	}

	if lm.dbWriter != nil {
		lm.dbWriter.PersistOrder(*existingEntry)
	}

	lm.broadcastOrderUpdate(*existingEntry)

	// Manage active position mapping tracking
	if o.FilledQuantity > 0 {
		symbolKey := strings.ToUpper(o.TradingSymbol)
		productKey := strings.ToUpper(o.Product)
		key := fmt.Sprintf("%s:%s", symbolKey, productKey)

		pos, exists := lm.activePositions[key]
		if !exists {
			pos = &models.Position{
				Symbol:        symbolKey,
				Product:       productKey,
				NetQuantity:   0,
				AveragePrice:  0,
				TargetPrice:   0,
				StopLossPrice: 0,
			}
			lm.activePositions[key] = pos
		}

		fillDelta := int(o.FilledQuantity) - previousFilledQty
		if fillDelta > 0 {
			sideUpper := strings.ToUpper(o.TransactionType)

			var tradeChange int
			if sideUpper == "BUY" {
				tradeChange = fillDelta
			} else {
				tradeChange = -fillDelta
			}

			var currentSignedQty int
			if pos.Side == "SHORT" {
				currentSignedQty = -pos.NetQuantity
			} else {
				currentSignedQty = pos.NetQuantity
			}

			executionFillPrice := o.AveragePrice
			if executionFillPrice == 0 {
				executionFillPrice = o.Price
			}

			// --- FIX 1: ACCUMULATE REALIZED PNL ON EXITS ---
			isBuy := sideUpper == "BUY"
			isReducing := (isBuy && currentSignedQty < 0) || (!isBuy && currentSignedQty > 0)

			if isReducing {
				closedQty := fillDelta
				if fillDelta > int(math.Abs(float64(currentSignedQty))) {
					closedQty = int(math.Abs(float64(currentSignedQty)))
				}

				var tradePnL float64
				if pos.Side == "LONG" {
					tradePnL = (executionFillPrice - pos.AveragePrice) * float64(closedQty)
				} else if pos.Side == "SHORT" {
					tradePnL = (pos.AveragePrice - executionFillPrice) * float64(closedQty)
				}
				pos.RealizedPnL += tradePnL
			}

			netSignedQty := currentSignedQty + tradeChange

			if netSignedQty > 0 {
				pos.Side = "LONG"
				pos.NetQuantity = netSignedQty

				if currentSignedQty >= 0 {
					totalCost := (float64(currentSignedQty) * pos.AveragePrice) + (float64(fillDelta) * executionFillPrice)
					pos.AveragePrice = totalCost / float64(netSignedQty)
				} else {
					pos.AveragePrice = executionFillPrice
				}
			} else if netSignedQty < 0 {
				pos.Side = "SHORT"
				pos.NetQuantity = -netSignedQty

				if currentSignedQty <= 0 {
					totalCost := (float64(-currentSignedQty) * pos.AveragePrice) + (float64(fillDelta) * executionFillPrice)
					pos.AveragePrice = totalCost / float64(-netSignedQty)
				} else {
					pos.AveragePrice = executionFillPrice
				}
			} else {
				pos.Side = ""
				pos.NetQuantity = 0
				pos.AveragePrice = 0
				pos.TargetPrice = 0
				pos.StopLossPrice = 0
				pos.UnrealizedPnL = 0
			}

			// --- FIX 2: INSTANTLY RECALCULATE UNREALIZED PNL ---
			// Don't wait for the next market tick to update the UI
			if ltp, hasLtp := lm.lastPrices[symbolKey]; hasLtp && pos.NetQuantity != 0 {
				if pos.Side == "LONG" {
					pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)
				} else if pos.Side == "SHORT" {
					pos.UnrealizedPnL = (pos.AveragePrice - ltp) * float64(pos.NetQuantity)
				}
			}
		}

		// 6. Absolute Squaring Off Cleanup Workflow Verification
		if pos.NetQuantity == 0 {
			pos.Side = ""
			pos.AveragePrice = 0
			// pos.RealizedPnL = 0  <--- DELETED: Never wipe out the profit history!
			pos.UnrealizedPnL = 0
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
		}

		if lm.dbWriter != nil {
			lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
		}

		lm.broadcastPositionUpdate(pos)
	}
}

// ============================================================================
// 3. Local Memory Position Risk Mutators
// ============================================================================

// UpdatePositionMetadata commits manual risk targets straight into localized position RAM map
func (lm *LiveOrderManager) UpdatePositionMetadata(symbol string, product string, tp float64, sl float64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	symbolKey := strings.ToUpper(symbol)
	key := fmt.Sprintf("%s:%s", symbolKey, strings.ToUpper(product))
	pos, exists := lm.activePositions[key]
	if !exists || pos.NetQuantity == 0 {
		return fmt.Errorf("cannot assign risk boundaries to an empty position for %s", symbol)
	}

	// Update RAM coordinates directly. 0 overrides past constraints to clear visual targets.
	pos.TargetPrice = tp
	pos.StopLossPrice = sl

	if lm.dbWriter != nil {
		lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
	}

	logger.Infof("[Live] In-Memory Risk Bounds Saved for %s: TP=%.2f, SL=%.2f", symbolKey, tp, sl)
	lm.broadcastPositionUpdate(pos)

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
	rawStatus := strings.ToUpper(o.Status)
	mappedStatus := rawStatus

	// Standardize all broker working execution frames safely to "PENDING" for your UI mapping
	if rawStatus == "OPEN" || rawStatus == "TRIGGER PENDING" || rawStatus == "UPDATE" || rawStatus == "PUT ORDER REQ RECEIVED" || rawStatus == "VALIDATION PENDING" {
		mappedStatus = "PENDING"
	}

	email := "bot.live@gidh.tech" // Fallback fallback string
	for _, entry := range lm.orderBook {
		if entry.OrderID == o.OrderID && entry.UserEmail != "" {
			email = entry.UserEmail
			break
		}
	}

	// Accurate Price Tracking Strategy
	var displayPrice float64
	if rawStatus == "COMPLETE" {
		if o.AveragePrice > 0 {
			displayPrice = o.AveragePrice
		} else {
			displayPrice = o.Price
		}
	} else {
		// If working/pending, track the live requested limit target
		displayPrice = o.Price
	}

	return models.OrderBookEntry{
		OrderID:   o.OrderID,
		Symbol:    strings.ToUpper(o.TradingSymbol),
		Side:      strings.ToUpper(o.TransactionType),
		OrderType: strings.ToUpper(o.OrderType),
		Qty:       int(o.Quantity),
		FilledQty: int(o.FilledQuantity),
		Price:     displayPrice,
		Status:    mappedStatus,
		Timestamp: o.OrderTimestamp.Time,
		UserEmail: email,
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

	// Track which keys were confirmed by the exchange
	exchangeKeys := make(map[string]bool)

	for _, pos := range positions.Net {
		symbolKey := strings.ToUpper(pos.Tradingsymbol)
		productKey := strings.ToUpper(pos.Product)
		key := fmt.Sprintf("%s:%s", symbolKey, productKey)
		exchangeKeys[key] = true

		// 1. If exchange confirms it's flat, explicitly neutralize our local structure
		if pos.Quantity == 0 {
			if localPos, exists := lm.activePositions[key]; exists {
				localPos.NetQuantity = 0
				localPos.Side = ""
				localPos.AveragePrice = 0
				localPos.UnrealizedPnL = 0
				localPos.TargetPrice = 0
				localPos.StopLossPrice = 0
				localPos.LastFillQty = 0

				if lm.dbWriter != nil {
					lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
				}
				lm.broadcastPositionUpdate(localPos)
			}
			continue
		}

		// 2. OVERWRITE LOGIC: Apply absolute state from Zerodha
		localPos, exists := lm.activePositions[key]
		if !exists {
			localPos = &models.Position{
				Symbol:  symbolKey,
				Product: productKey,
			}
			lm.activePositions[key] = localPos
		}

		// Directly overwrite the local quantity with Zerodha's absolute truth
		localPos.NetQuantity = int(math.Abs(float64(pos.Quantity)))
		if pos.Quantity > 0 {
			localPos.Side = "LONG"
		} else {
			localPos.Side = "SHORT"
		}

		// --- FIX: Stop overwriting accurate open-leg prices with day-averaged prices ---
		// Only adopt Zerodha's API average price if we don't currently have a local one
		if localPos.AveragePrice == 0 {
			localPos.AveragePrice = pos.AveragePrice
		}

		// Flush the corrected state to the database and broadcast
		if lm.dbWriter != nil {
			lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
		}
		lm.broadcastPositionUpdate(localPos)

		logger.Infof("[Sync] Successfully recovered live active position tracking: %s Qty %d at %.2f", pos.Tradingsymbol, pos.Quantity, localPos.AveragePrice)
	}

	// 3. Optional: Wipe out local ghost entries that do not exist in the broker's portfolio response at all
	for key, localPos := range lm.activePositions {
		if !exchangeKeys[key] && localPos.NetQuantity != 0 {
			localPos.NetQuantity = 0
			localPos.Side = ""
			localPos.AveragePrice = 0
			localPos.TargetPrice = 0
			localPos.StopLossPrice = 0

			if lm.dbWriter != nil {
				lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
			}
			lm.broadcastPositionUpdate(localPos)
		}
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
		// 1. Create a copy so we don't mutate the live memory map during a Read lock
		posCopy := *pos

		// 2. Check if we have a live price cached for this symbol
		if ltp, exists := lm.lastPrices[posCopy.Symbol]; exists && posCopy.NetQuantity != 0 {

			// 3. Calculate the absolute real-time PnL for the exact millisecond of the UI request
			if posCopy.Side == "LONG" {
				posCopy.UnrealizedPnL = (ltp - posCopy.AveragePrice) * float64(posCopy.NetQuantity)
			} else if posCopy.Side == "SHORT" {
				posCopy.UnrealizedPnL = (posCopy.AveragePrice - ltp) * float64(posCopy.NetQuantity)
			}
		}

		positions = append(positions, posCopy)
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

// ReconstituteState rehydrates active live exposures and session logs into RAM upon a backend crash or restart.
func (lm *LiveOrderManager) ReconstituteState(orders []models.OrderBookEntry, positions []models.Position) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// 1. Rehydrate the underlying order book ledger for the current session
	lm.orderBook = orders

	// 2. Scan and rehydrate only unhedged/open risk entries directly into the memory container
	for i := range positions {
		pos := positions[i]

		// The frontend terminal explicitly tracks protection borders for slots with active exposure
		if pos.NetQuantity != 0 {
			key := fmt.Sprintf("%s:%s", strings.ToUpper(pos.Symbol), strings.ToUpper(pos.Product))

			// Allocate memory and copy values to avoid structural slice alignment leaks
			lm.activePositions[key] = &models.Position{
				Symbol:        pos.Symbol,
				Product:       pos.Product,
				Side:          pos.Side,
				NetQuantity:   pos.NetQuantity,
				AveragePrice:  pos.AveragePrice,
				RealizedPnL:   pos.RealizedPnL,
				UnrealizedPnL: pos.UnrealizedPnL,
				TargetPrice:   pos.TargetPrice,   // Restores the exact horizontal boundary coordinate
				StopLossPrice: pos.StopLossPrice, // Restores the exact floor line coordinate
			}

			logger.Infof("[Live Startup] Reconstituted active engine map for %s: NetQty=%d, SL=%.2f, TP=%.2f",
				key, pos.NetQuantity, pos.StopLossPrice, pos.TargetPrice)
		}
	}
	logger.Infof("[Live Startup] Memory footprint restoration complete. %d orders and %d active positions tracked.", len(lm.orderBook), len(lm.activePositions))
}

// RoundToTick safely conforms floating price inputs to valid exchange tick step boundaries
func RoundToTick(price float64, tickSize float64) float64 {
	if tickSize <= 0 {
		return price
	}
	return math.Round(price/tickSize) * tickSize
}

// GetTickSizeForSymbol returns the correct tick size based on NSE Equity conventions
func GetTickSizeForSymbol(symbol string, targetPrice float64) float64 {
	sym := strings.ToUpper(symbol)

	// 1. Explicitly check for Liquid/Bees ETFs
	if strings.Contains(sym, "LIQUID") || strings.Contains(sym, "CASE") || strings.Contains(sym, "BEES") {
		return 1.00
	}

	// 2. NSE Tick Size Rules (Implemented April 2025)
	if targetPrice < 250 {
		return 0.01
	} else if targetPrice < 1000 {
		return 0.05
	} else if targetPrice < 5000 {
		return 0.10
	} else if targetPrice < 10000 {
		return 0.50
	} else if targetPrice < 20000 {
		return 1.00
	} else {
		return 5.00
	}
}
