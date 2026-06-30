package order

import (
	"context"
	"fmt"
	"math"
	"sort"
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
	mu                     sync.RWMutex
	kiteClient             *kiteconnect.Client
	dbWriter               *writer.DBWriter
	wsHub                  *ws.Hub
	activePositions        map[string]*models.Position
	orderBook              []models.OrderBookEntry
	lastPrices             map[string]float64
	positionChangeCallback func(symbol string, netQty int, side string, avgPrice float64, realizedPnL float64)
	skipExecution          bool
}

func NewLiveOrderManager(kc *kiteconnect.Client, hub *ws.Hub, db *writer.DBWriter, skipExec bool) *LiveOrderManager {
	return &LiveOrderManager{
		kiteClient:             kc,
		dbWriter:               db,
		wsHub:                  hub,
		activePositions:        make(map[string]*models.Position),
		orderBook:              make([]models.OrderBookEntry, 0),
		lastPrices:             make(map[string]float64),
		positionChangeCallback: nil,
		skipExecution:          skipExec,
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

	// 🛑 DRY RUN INTERCEPTOR GATEWAY
	if lm.skipExecution {
		mockOrderID := fmt.Sprintf("DRY-%d", time.Now().UnixNano())
		logger.Warnf("🔬 [DRY RUN MODE] Intercepted routing ticket for %s: %s %d shares.", req.Symbol, req.TransactionType, req.Quantity)

		ltp := lm.lastPrices[strings.ToUpper(req.Symbol)]
		if ltp <= 0 {
			ltp = req.Price // Fallback to requested coordinate if tick feed hasn't arrived
		}

		lm.mu.Lock()
		dryEntry := models.OrderBookEntry{
			OrderID:   mockOrderID,
			Symbol:    strings.ToUpper(req.Symbol),
			Side:      strings.ToUpper(req.TransactionType),
			OrderType: strings.ToUpper(req.OrderType),
			Qty:       req.Quantity,
			FilledQty: req.Quantity, // Assume absolute immediate fill for tracking logic parity
			Price:     ltp,
			Status:    "COMPLETE",
			Timestamp: time.Now(),
			UserEmail: req.UserEmail,
		}
		lm.orderBook = append(lm.orderBook, dryEntry)

		if lm.dbWriter != nil {
			lm.dbWriter.PersistOrder(dryEntry)
		}
		lm.mu.Unlock()

		lm.broadcastOrderUpdate(dryEntry)

		// Trigger background synchronization logic immediately to populate local position RAM caches
		// and fire our callback downstream to the RiskManager/StrategyEngine layers!
		go func(symbol string) {
			_, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// If dry running, SyncExchangeState cannot pull matching logs from Zerodha.
			// We trigger an explicit manual evaluation pass or let background mock state handle it.
			// Better yet, trigger the change callback directly for structural simulation correctness:
			lm.mu.Lock()
			// Basic mock position calculations inside live tracking slots for dry run mode:
			key := fmt.Sprintf("%s:MIS", strings.ToUpper(symbol))
			localPos, exists := lm.activePositions[key]
			if !exists {
				localPos = &models.Position{Symbol: strings.ToUpper(symbol), Product: "MIS"}
				lm.activePositions[key] = localPos
			}

			if localPos.NetQuantity == 0 {
				localPos.EntryTimestamp = dryEntry.Timestamp.UTC().Format(time.RFC3339)
			}

			// Simple signed adjustment calculation mapping
			multiplier := 1
			if dryEntry.Side == "SELL" {
				multiplier = -1
			}

			localPos.NetQuantity += dryEntry.Qty * multiplier
			if localPos.NetQuantity > 0 {
				localPos.Side = "LONG"
			} else if localPos.NetQuantity < 0 {
				localPos.Side = "SHORT"
			} else {
				localPos.Side = "FLAT"
			}
			localPos.AveragePrice = ltp

			netQty, side, avgPrice := localPos.NetQuantity, localPos.Side, localPos.AveragePrice
			lm.mu.Unlock()

			if lm.positionChangeCallback != nil {
				lm.positionChangeCallback(symbol, netQty, side, avgPrice, 0.0)
			}
		}(req.Symbol)

		return mockOrderID, nil
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
			pos.LTP = ltp
			// ⚡ FIX: A signed NetQuantity makes PnL math universally simple for both sides!
			pos.UnrealizedPnL = (ltp - pos.AveragePrice) * float64(pos.NetQuantity)

			if pos.EntryTimestamp != "" {
				if entryTime, err := time.Parse(time.RFC3339, pos.EntryTimestamp); err == nil {
					duration := time.Now().UTC().Sub(entryTime.UTC())
					pos.TimeElapsed = duration.Round(time.Second).String()
				}
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

				// ⚡ FIX: Extract absolute quantity ONLY right before firing the physical exchange order
				absQty := int(math.Abs(float64(pos.NetQuantity)))
				go lm.executeBrokerMarketLiquidation(pos.Symbol, pos.Product, pos.Side, absQty)

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

	mappedStatus := rawStatus
	if rawStatus == "OPEN" || rawStatus == "TRIGGER PENDING" || rawStatus == "UPDATE" || rawStatus == "PUT ORDER REQ RECEIVED" || rawStatus == "VALIDATION PENDING" {
		mappedStatus = "PENDING"
	}

	logger.Infof("[Live Engine] Processing broker update for OrderID: %s, Status: %s -> %s (Filled: %d/%d)",
		o.OrderID, rawStatus, mappedStatus, int(o.FilledQuantity), int(o.Quantity))

	var existingEntry *models.OrderBookEntry
	for i := range lm.orderBook {
		if lm.orderBook[i].OrderID == o.OrderID {
			existingEntry = &lm.orderBook[i]
			break
		}
	}

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

	// Visual Fix: Ensure pending Market Orders show the live price instead of $0 in the UI grid
	if displayPrice <= 0 {
		if ltp, hasLtp := lm.lastPrices[strings.ToUpper(o.TradingSymbol)]; hasLtp && ltp > 0 {
			displayPrice = ltp
		}
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

	// --- FIX: LEVERAGE ZERODHA'S CALCULATION PIPELINE ON EXECUTIONS ---
	// Whenever an order registers a partial or complete fill, we dispatch a background
	// synchronization request. This pulls the absolute reality (especially finalized Realized PnL
	// when a position drops back down to 0) directly from Zerodha's clearing server.
	if o.FilledQuantity > 0 {
		go func(symbol string) {
			// Give the API call a healthy network threshold window to execute safely
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			logger.Infof("[PnL Sync Engine] Trade execution detected for %s. Synchronizing verified position state...", symbol)

			// This invokes the "Ultimate Healer" block in SyncExchangeState, updating your maps,
			// writing flawless metrics to your trade database, and pumping clean JSON to your UI stream.
			if err := lm.SyncExchangeState(ctx); err != nil {
				logger.Errorf("[PnL Sync Engine] Mid-session recovery sync aborted: %v", err)
			}
		}(o.TradingSymbol)
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
	if rawStatus == "COMPLETE" || o.FilledQuantity > 0 {
		if o.AveragePrice > 0 {
			displayPrice = o.AveragePrice
		} else {
			displayPrice = o.Price
		}
	} else {
		// If working/pending with no fills yet, track the live requested limit target
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

	type posChangeEvent struct {
		symbol      string
		netQty      int
		side        string
		avgPrice    float64
		realizedPnL float64
	}
	var eventsToDispatch []posChangeEvent

	lm.mu.Lock()
	defer func() {
		lm.mu.Unlock()
		if lm.positionChangeCallback != nil && len(eventsToDispatch) > 0 {
			for _, ev := range eventsToDispatch {
				go lm.positionChangeCallback(ev.symbol, ev.netQty, ev.side, ev.avgPrice, ev.realizedPnL)
			}
		}
	}()

	exchangeKeys := make(map[string]bool)

	for _, pos := range positions.Net {
		symbolKey := strings.ToUpper(pos.Tradingsymbol)
		productKey := strings.ToUpper(pos.Product)
		key := fmt.Sprintf("%s:%s", symbolKey, productKey)
		exchangeKeys[key] = true

		// ⚡ FIX: Seed the local price map with the broker's snapshot LastPrice!
		// This ensures that the very first REST API invocation on page load works right away.
		if pos.LastPrice > 0 {
			lm.lastPrices[symbolKey] = pos.LastPrice
		}

		// 1. Position is Flat
		if pos.Quantity == 0 {
			localPos, exists := lm.activePositions[key]
			if !exists {
				localPos = &models.Position{Symbol: symbolKey, Product: productKey}
				lm.activePositions[key] = localPos
			}

			localPos.NetQuantity = 0
			localPos.Side = "FLAT"
			localPos.AveragePrice = 0
			localPos.UnrealizedPnL = 0
			localPos.TargetPrice = 0
			localPos.StopLossPrice = 0
			localPos.LastFillQty = 0
			localPos.RealizedPnL = pos.PnL

			if lm.dbWriter != nil {
				lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
			}
			lm.broadcastPositionUpdate(localPos)

			eventsToDispatch = append(eventsToDispatch, posChangeEvent{
				symbol:      symbolKey,
				netQty:      0,
				side:        "FLAT",
				avgPrice:    0.0,
				realizedPnL: pos.PnL,
			})
			continue
		}

		// 2. Open Active Position
		localPos, exists := lm.activePositions[key]
		if !exists {
			localPos = &models.Position{Symbol: symbolKey, Product: productKey}
			lm.activePositions[key] = localPos
		}

		localPos.NetQuantity = pos.Quantity
		if pos.Quantity > 0 {
			localPos.Side = "LONG"
		} else {
			localPos.Side = "SHORT"
		}

		localPos.AveragePrice = lm.calculateTrueAveragePrice(symbolKey, pos.Quantity)
		localPos.RealizedPnL = pos.Realised

		// ⚡ FIX: Explicitly compute Unrealized PnL during baseline initialization state sync
		if pos.LastPrice > 0 {
			localPos.UnrealizedPnL = (pos.LastPrice - localPos.AveragePrice) * float64(localPos.NetQuantity)
			localPos.UnrealizedPnL = math.Round(localPos.UnrealizedPnL*100) / 100
		}

		if lm.dbWriter != nil {
			lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
		}
		lm.broadcastPositionUpdate(localPos)

		eventsToDispatch = append(eventsToDispatch, posChangeEvent{
			symbol:      symbolKey,
			netQty:      localPos.NetQuantity,
			side:        localPos.Side,
			avgPrice:    localPos.AveragePrice,
			realizedPnL: pos.Realised,
		})

		logger.Infof("[Sync] Successfully recovered live active position tracking: %s Qty %d at %.2f", pos.Tradingsymbol, pos.Quantity, localPos.AveragePrice)
	}

	// 3. Clean up ghost local entries
	for key, localPos := range lm.activePositions {
		if !exchangeKeys[key] && localPos.NetQuantity != 0 {
			localPos.NetQuantity = 0
			localPos.Side = "FLAT"
			localPos.AveragePrice = 0
			localPos.TargetPrice = 0
			localPos.StopLossPrice = 0

			if lm.dbWriter != nil {
				lm.dbWriter.PersistPositionSnapshot(localPos, time.Now())
			}
			lm.broadcastPositionUpdate(localPos)

			eventsToDispatch = append(eventsToDispatch, posChangeEvent{
				symbol:      localPos.Symbol,
				netQty:      0,
				side:        "FLAT",
				avgPrice:    0.0,
				realizedPnL: localPos.RealizedPnL,
			})
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
		posCopy := *pos

		if ltp, exists := lm.lastPrices[posCopy.Symbol]; exists && ltp > 0 {
			posCopy.LTP = ltp
		}

		if posCopy.NetQuantity != 0 {
			if ltp, exists := lm.lastPrices[posCopy.Symbol]; exists && ltp > 0 {
				posCopy.UnrealizedPnL = (ltp - posCopy.AveragePrice) * float64(posCopy.NetQuantity)
				posCopy.UnrealizedPnL = math.Round(posCopy.UnrealizedPnL*100) / 100
			} else {
				// ⚡ FIX: If no fresh WebSocket tick has arrived yet, do not let it drop to 0!
				// Retain the initialized value computed during SyncExchangeState.
				// If it's still 0, we can use posCopy.AveragePrice as a break-even baseline until tick 1.
				if posCopy.UnrealizedPnL == 0 {
					// Optionally look up a default fallback matrix here or keep baseline
				}
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

// calculateTrueAveragePrice reconstructs the true FIFO entry average of the CURRENT active position.
// It ignores closed intraday cycles that pollute the broker's daily blended average.
// Assumes lm.mu is already locked by the caller.
func (lm *LiveOrderManager) calculateTrueAveragePrice(symbol string, netQuantity int) float64 {
	if netQuantity == 0 {
		return 0
	}

	targetSide := "BUY"
	if netQuantity < 0 {
		targetSide = "SELL"
	}

	absNet := int(math.Abs(float64(netQuantity)))
	gatheredQty := 0
	totalValue := 0.0

	var fills []models.OrderBookEntry
	for _, o := range lm.orderBook {
		if o.Symbol == symbol && o.Side == targetSide && o.FilledQty > 0 {
			fills = append(fills, o)
		}
	}

	// Sort descending by timestamp (newest fills first)
	sort.Slice(fills, func(i, j int) bool {
		return fills[i].Timestamp.After(fills[j].Timestamp)
	})

	for _, fill := range fills {
		needed := absNet - gatheredQty
		if needed <= 0 {
			break
		}
		take := fill.FilledQty
		if take > needed {
			take = needed
		}
		totalValue += float64(take) * fill.Price
		gatheredQty += take
	}

	if gatheredQty == 0 {
		return 0
	}
	return totalValue / float64(gatheredQty)
}

func (lm *LiveOrderManager) RegisterPositionChangeCallback(cb func(symbol string, netQty int, side string, avgPrice float64, realizedPnL float64)) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.positionChangeCallback = cb
}
