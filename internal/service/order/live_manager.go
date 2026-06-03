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

// HandleOrderUpdate processes real-time execution updates streamed asynchronously from Zerodha Kite Connect.
// This preserves localized memory risk boundaries while syncing execution changes down to the UI stores.
func (lm *LiveOrderManager) HandleOrderUpdate(o kiteconnect.Order) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	statusUpper := strings.ToUpper(o.Status)
	logger.Infof("[Live Engine] Processing broker update for OrderID: %s, Status: %s (Filled: %d/%d)",
		o.OrderID, statusUpper, o.FilledQuantity, o.Quantity)

	var existingEntry *models.OrderBookEntry

	// 1. Locate the existing entry inside our localized session cache book
	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == o.OrderID {
			existingEntry = &lm.orderBook[i]
			break
		}
	}

	// 2. If the order entity doesn't exist yet (e.g. execution frame caught right at boot), allocate it in cache
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
			Price:     o.Price,
			Status:    statusUpper,
			Timestamp: o.OrderTimestamp.Time,
			UserEmail: email,
		}
		if newEntry.Timestamp.IsZero() {
			newEntry.Timestamp = time.Now()
		}
		lm.orderBook = append(lm.orderBook, newEntry)
		existingEntry = &lm.orderBook[len(lm.orderBook)-1]
	} else {
		// Update core incremental tracking variables safely via the active map pointer
		existingEntry.FilledQty = int(o.FilledQuantity)
		existingEntry.Status = statusUpper
		if o.Price > 0 {
			existingEntry.Price = o.Price
		}
	}

	// 3. Persist entry audit state directly to the SQL database tables
	if lm.dbWriter != nil {
		lm.dbWriter.PersistOrder(*existingEntry)
	}

	// 4. Dispatch Event Frame A to keep frontend execution meters perfectly synchronized
	lm.broadcastOrderUpdate(*existingEntry)

	// 5. Manage active position mapping tracking if an order has registered fills
	if o.FilledQuantity > 0 {
		symbolKey := strings.ToUpper(o.TradingSymbol)
		productKey := strings.ToUpper(o.Product)
		key := fmt.Sprintf("%s:%s", symbolKey, productKey)

		pos, exists := lm.activePositions[key]
		if !exists {
			// Allocate a brand-new live position box.
			// We intentionally start risk boundaries at 0.00 to avoid client guessing games.
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

		// Calculate matching fills safely using last fill differences
		// This protects position calculation logic if Kite fires multiple partial fill frames sequentially
		fillDelta := int(o.FilledQuantity) - pos.LastFillQty
		if fillDelta > 0 {
			sideUpper := strings.ToUpper(o.TransactionType)

			// Determine the signed change from this specific update execution frame
			var tradeChange int
			if sideUpper == "BUY" {
				tradeChange = fillDelta
			} else {
				tradeChange = -fillDelta
			}

			// Convert the existing localized state into a true signed integer exposure
			var currentSignedQty int
			if pos.Side == "SHORT" {
				currentSignedQty = -pos.NetQuantity
			} else {
				currentSignedQty = pos.NetQuantity
			}

			// Compute the accurate net target exposure
			netSignedQty := currentSignedQty + tradeChange

			if netSignedQty > 0 {
				pos.Side = "LONG"
				pos.NetQuantity = netSignedQty
				// Recalculate average price only if expanding execution vectors
				if currentSignedQty >= 0 {
					totalCost := (float64(currentSignedQty) * pos.AveragePrice) + (float64(fillDelta) * o.Price)
					pos.AveragePrice = totalCost / float64(netSignedQty)
				}
			} else if netSignedQty < 0 {
				pos.Side = "SHORT"
				pos.NetQuantity = -netSignedQty // Keep NetQuantity as an absolute positive value
				if currentSignedQty <= 0 {
					totalCost := (float64(-currentSignedQty) * pos.AveragePrice) + (float64(fillDelta) * o.Price)
					pos.AveragePrice = totalCost / float64(-netSignedQty)
				}
			} else {
				// Correctly clear out all spatial parameters when the position neutralizes perfectly
				pos.Side = ""
				pos.NetQuantity = 0
				pos.AveragePrice = 0
				pos.TargetPrice = 0
				pos.StopLossPrice = 0
				pos.UnrealizedPnL = 0
			}

			// Cache historical fill markers to handle consecutive calculation packages smoothly
			pos.LastFillQty = int(o.FilledQuantity)
		}

		// 6. Absolute Squaring Off Cleanup Workflow Checklist Verification
		// If a position returns to 0 shares, clear risk allocations instantly to wipe chart lines
		if pos.NetQuantity == 0 {
			pos.Side = ""
			pos.AveragePrice = 0
			pos.RealizedPnL = 0
			pos.UnrealizedPnL = 0
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
			pos.LastFillQty = 0
		}

		// 7. Flush the position state box down into the core persistent hypertables
		if lm.dbWriter != nil {
			lm.dbWriter.PersistPositionSnapshot(pos, time.Now())
		}

		// 8. Dispatch Event Frame B to immediately refresh client dashboards or wipe ghost chart lines
		lm.broadcastPositionUpdate(pos)
	}

	// Reset fill state counters if the underlying entry has fully finalized its execution path
	if statusUpper == "COMPLETE" || statusUpper == "CANCELLED" || statusUpper == "REJECTED" {
		if existingEntry != nil {
			// Zero out position reference fields for future order tickets
			symbolKey := strings.ToUpper(o.TradingSymbol)
			productKey := strings.ToUpper(o.Product)
			key := fmt.Sprintf("%s:%s", symbolKey, productKey)
			if pos, exists := lm.activePositions[key]; exists {
				pos.LastFillQty = 0
			}
		}
	}
}

// Simple absolute value helper function for integer calculations
func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
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
	statusUpper := strings.ToUpper(o.Status)
	status := statusUpper

	// Standardize all broker working execution frames safely to "PENDING" for your UI mapping
	if statusUpper == "OPEN" || statusUpper == "TRIGGER PENDING" || statusUpper == "UPDATE" || statusUpper == "PUT ORDER REQ RECEIVED" || statusUpper == "VALIDATION PENDING" {
		status = "PENDING"
	}

	email := "bot.live@gidh.tech" // Fallback fallback string
	for _, entry := range lm.orderBook {
		if entry.OrderID == o.OrderID && entry.UserEmail != "" {
			email = entry.UserEmail
			break
		}
	}

	return models.OrderBookEntry{
		OrderID:   o.OrderID,
		Symbol:    strings.ToUpper(o.TradingSymbol),
		Side:      strings.ToUpper(o.TransactionType),
		OrderType: strings.ToUpper(o.OrderType),
		Qty:       int(o.Quantity),
		FilledQty: int(o.FilledQuantity),
		Price:     o.Price,
		Status:    status, // Safely outputs "PENDING" to your web application UI
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

		// If exchange confirms it's flat, explicitly neutralize our local structure
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

		txType := kiteconnect.TransactionTypeBuy
		if pos.Quantity < 0 {
			txType = kiteconnect.TransactionTypeSell
		}

		lm.updatePositionStateFromFill(pos.Tradingsymbol, pos.Product, txType, int(math.Abs(float64(pos.Quantity))), pos.AveragePrice)
		logger.Infof("[Sync] Successfully recovered live active position tracking: %s Qty %d", pos.Tradingsymbol, pos.Quantity)
	}

	// Optional: Wipe out local ghost entries that do not exist in the broker's portfolio response at all
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

	// 1. Explicitly check for Liquid/Bees ETFs (Always 1.00)
	if strings.Contains(sym, "LIQUID") || strings.Contains(sym, "CASE") || strings.Contains(sym, "BEES") {
		return 1.00
	}

	// 2. Extract the remaining fractional component of the raw target price
	_, fraction := math.Modf(targetPrice)
	fraction = math.Round(fraction*100) / 100 // Clean up tiny float precision errors (e.g., 0.49999)

	// 3. Dynamic Auto-Detection if the UI sent an exact half-rupee or full-rupee decimal
	if fraction == 0.50 {
		return 0.50
	} else if fraction == 0.00 {
		// If the UI sends a flat round number, we can look for specific keywords or default safely to 0.05.
		// Since 0.05 fits perfectly into 0.50 and 1.00 mathematically, using 0.05 as a fallback is clean.
		return 0.05
	}

	// Standard baseline for 99% of all regular NSE Equity MIS stocks
	return 0.05
}
