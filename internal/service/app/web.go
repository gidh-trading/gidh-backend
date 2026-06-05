package app

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/reader"
	"gidh-backend/internal/service/stream"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"
)

// initWebServer sets up the WebSocket hub and HTTP routes.
func (a *App) initWebServer() {
	a.wsHub = ws.NewHub()
	go a.wsHub.Run()

	mux := http.NewServeMux()
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWs(a.wsHub, w, r)
	})

	mux.HandleFunc("/api/backtest/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// The StreamManager knows the current processing state
		currentDate := ""
		if a.StreamManager != nil {
			currentDate = a.StreamManager.GetStatus()
		}

		json.NewEncoder(w).Encode(map[string]interface{}{
			"mode":       a.Config.Mode,
			"date":       currentDate,
			"is_running": a.StreamManager != nil,
		})
	})
	mux.HandleFunc("/api/backtest/start", a.handleBacktestStart)
	mux.HandleFunc("/api/backtest/stop", a.handleBacktestStop)
	mux.HandleFunc("/api/backtest/speed", a.handleBacktestSpeedUpdate)
	mux.HandleFunc("/api/alerts", a.handleGetAlerts)

	// --- LOCAL REFACTORED CHANNELS ---
	mux.HandleFunc("/api/orders/place", a.handleOrderPlace)
	mux.HandleFunc("/api/orders/modify", a.handleOrderModify)
	mux.HandleFunc("/api/orders/cancel", a.handleOrderCancel)
	mux.HandleFunc("/api/positions/metadata", a.handleUpdateExits)
	mux.HandleFunc("/api/positions/exit", a.handlePositionExit)

	mux.HandleFunc("/api/orders/", a.handleGetHistoricalOrders)
	mux.HandleFunc("/api/positions/history/", a.handleGetHistoricalPositions)

	mux.HandleFunc("/api/internal/backtest/vcn/all", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		if a.RiskManager == nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			json.NewEncoder(w).Encode(map[string]string{
				"error": "Risk manager instance is inactive or application is running in live mode",
			})
			return
		}

		// Pull the granular payload directly matching the UI Contract note signature
		payload := a.RiskManager.GetUIContractNote()

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(payload)
	})

	handlerWithLogging := LoggingMiddleware(mux)

	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.Port),
		Handler: handlerWithLogging,
	}
}

func (a *App) handleBacktestStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req StartBacktestRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	ctx := r.Context()

	// 1. Stop existing manager if running
	a.managerMu.Lock()
	if a.StreamManager != nil {
		logger.Info("Stopping existing stream for new backtest request...")
		a.StreamManager.Stop()
	}

	// 2. Update DB selection for stocks
	instReader := reader.NewInstrumentReader(a.pool)
	if err := instReader.UpdateBacktestSelection(ctx, req.Stocks); err != nil {
		a.managerMu.Unlock()
		logger.Errorf("Failed to update backtest selection: %v", err)
		http.Error(w, "Failed to update stock selection", http.StatusInternalServerError)
		return
	}

	// 3. Prepare Data (Extract .tar.xz)
	if err := stream.PrepareBacktestData(a.Config.BacktestBackupDir, a.Config.BacktestDataDir, req.Date); err != nil {
		a.managerMu.Unlock()
		logger.Errorf("Data preparation failed: %v", err)
		http.Error(w, "Backtest data not found or extraction failed", http.StatusNotFound)
		return
	}

	// 4. Cleanup DB for the new date
	if err := db.CleanupBacktestData(ctx, req.Date); err != nil {
		a.managerMu.Unlock()
		logger.Errorf("CRITICAL: Backtest cleanup failed! Aborting run to protect data integrity: %v", err)
		http.Error(w, fmt.Sprintf("Failed to clear database before backtest: %v", err), http.StatusInternalServerError)
		return
	}

	// 5. Override Config with API params
	a.Config.BacktestDate = req.Date
	a.Config.BacktestSpeedFactor = req.SpeedFactor

	// 6. Reload Market Data & DNA for the specific date
	parsedDate, _ := time.Parse("2006-01-02", req.Date)
	dnaMap, profilesMap, potentialsMatrix := a.loadMarketData(ctx, parsedDate)

	// 7. RE-INITIALIZE PIPELINE (This resets all internal memory/maps)
	if err := a.initPipeline(ctx, dnaMap, profilesMap, potentialsMatrix); err != nil {
		a.managerMu.Unlock()
		http.Error(w, "Pipeline init failed", http.StatusInternalServerError)
		return
	}

	// 8. Re-init Stream Manager with new source
	if err := a.initStreamManager(); err != nil {
		a.managerMu.Unlock()
		http.Error(w, "Stream Manager init failed", http.StatusInternalServerError)
		return
	}

	// 9. Start Processing
	go func() {
		if err := a.StreamManager.Start(); err != nil {
			logger.Errorf("Stream started with error: %v", err)
		}
	}()

	a.managerMu.Unlock()

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "started", "date": req.Date})
}

func (a *App) handleBacktestStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.managerMu.Lock()
	defer a.managerMu.Unlock()

	if a.StreamManager != nil {
		logger.Info("Stopping stream manager via API request...")

		// 1. First stop the streaming data layers.
		// This cancels contexts and blocks until ALL workers exit runProcessor().
		// This guarantees NO background goroutines are accessing the pipeline structures.
		a.StreamManager.Stop()

		// 2. Safely clear the position matrix now that concurrent access is terminated.
		if a.OrderManager != nil {
			a.OrderManager.ClearPositions()
		}

		// 3. Reset the pipeline architecture and maps safely without background race conditions.
		if a.Pipeline != nil {
			a.Pipeline.Reset()
		}

		// 4. Detach managers
		a.StreamManager = nil
		a.activeManager = nil

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "stopped",
			"message": "Stream and trade state terminated successfully",
		})
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "idle",
			"message": "No active backtest to stop",
		})
	}
}

func (a *App) handleBacktestSpeedUpdate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req UpdateSpeedRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	a.managerMu.Lock()
	defer a.managerMu.Unlock()

	if a.StreamManager == nil {
		http.Error(w, "No active backtest running to change speed", http.StatusBadRequest)
		return
	}

	// Dynamic Type Assertion to find our BacktestSource implementation safely
	// inside the active stream manager
	if backtestSrc, ok := a.StreamManager.GetSource().(*stream.BacktestSource); ok {
		backtestSrc.SetSpeedFactor(req.SpeedFactor)
		a.Config.BacktestSpeedFactor = req.SpeedFactor // keep global config state in sync

		logger.Infof("Backtest speed factor adjusted mid-run to: %.2f", req.SpeedFactor)

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":        "success",
			"current_speed": req.SpeedFactor,
		})
		return
	}

	http.Error(w, "Speed updates are only supported in backtest mode", http.StatusBadRequest)
}

func (a *App) handleGetAlerts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Extract token if provided as a query string modifier (e.g. /api/alerts?token=123)
	tokenStr := r.URL.Query().Get("token")
	var history []pipeline.ScoutHistoricalSnapshot

	a.managerMu.Lock()
	pipelineValid := (a.Pipeline != nil && a.Pipeline.scoutStage != nil)

	if pipelineValid {
		if tokenStr != "" {
			tokenUint, err := strconv.ParseUint(tokenStr, 10, 32)
			if err != nil {
				a.managerMu.Unlock()
				logger.Errorf("GetAlerts Parse Error: Invalid token query parameter format '%s': %v", tokenStr, err)
				w.WriteHeader(http.StatusBadRequest)
				json.NewEncoder(w).Encode(map[string]string{"error": "Invalid instrument token parameter format"})
				return
			}
			// 🟢 Maintains full, detailed history for charting single asset lines
			history = a.Pipeline.scoutStage.GetAlertHistory(uint32(tokenUint))
		} else {
			// 🟢 FIXED: Calls your new filtered method so the initial Watchtower table render
			// loads only the active anomalies, completely wiping out historical row duplication!
			history = a.Pipeline.scoutStage.GetAllAlertHistory()
		}
	} else {
		history = []pipeline.ScoutHistoricalSnapshot{}
	}
	a.managerMu.Unlock()

	response := map[string]any{
		"status": "success",
		"data":   history,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Errorf("Web Server Error: Failed to serialize history array: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (a *App) handleOrderPlace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	orderID, err := a.OrderManager.PlaceOrder(r.Context(), req)
	if err != nil {
		logger.Errorf("Order Execution Failed: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"order_id": orderID,
	})
}
func (a *App) handleOrderModify(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req ModifyOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	if req.OrderID == "" {
		http.Error(w, "order_id is required", http.StatusBadRequest)
		return
	}

	// ⚡ FIXED: Route purely the order modification variables down to the interface
	err := a.OrderManager.ModifyOrder(req.OrderID, req.Price, req.UserEmail)
	if err != nil {
		logger.Errorf("Order Modification Failed: %v | OrderID: %s", err, req.OrderID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"message":  "Order modified successfully",
		"order_id": req.OrderID,
	})
}
func (a *App) handleOrderCancel(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req CancelOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	err := a.OrderManager.CancelOrder(req.OrderID, req.UserEmail)
	if err != nil {
		logger.Errorf("Order Cancellation Failed: %v | OrderID: %s", err, req.OrderID)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":   "success",
		"message":  "Order cancelled successfully",
		"order_id": req.OrderID,
	})
}
func (a *App) handleUpdateExits(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Symbol        string  `json:"symbol"`
		Product       string  `json:"product"`
		TargetPrice   float64 `json:"target_price"`
		StopLossPrice float64 `json:"stop_loss_price"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	product := req.Product
	if product == "" {
		product = "MIS"
	}

	// ⚡ FIXED: Commits the manual risk targets straight into localized position RAM map
	err := a.OrderManager.UpdatePositionMetadata(req.Symbol, product, req.TargetPrice, req.StopLossPrice)
	if err != nil {
		logger.Errorf("Failed to update position local metrics: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Local position parameters updated successfully",
	})
}
func (a *App) handlePositionExit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Symbol    string `json:"symbol"`
		Product   string `json:"product"`
		Quantity  int    `json:"quantity"`
		UserEmail string `json:"user_email"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	product := req.Product
	if product == "" {
		product = "MIS"
	}

	err := a.OrderManager.ExitPosition(r.Context(), req.Symbol, product, req.Quantity, req.UserEmail)
	if err != nil {
		logger.Errorf("Manual position liquidation routing encountered error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "success",
		"message": "Market liquidation execution triggered successfully",
	})
}
func (a *App) handleGetHistoricalOrders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1. Path sanitization wrapper matching the UI URL layout
	pathOnly := r.URL.Path
	if len(pathOnly) <= len("/api/orders/") {
		logger.Errorf("Historical Orders API Error: Missing date parameter in URL path: %s", pathOnly)
		http.Error(w, "date query parameter identifier is mandatory", http.StatusBadRequest)
		return
	}
	dateStr := pathOnly[len("/api/orders/"):]
	if idx := strings.Index(dateStr, "?"); idx != -1 {
		dateStr = dateStr[:idx]
	}
	dateStr = strings.TrimSpace(strings.TrimSuffix(dateStr, "/"))

	if dateStr == "" {
		logger.Error("Historical Orders API Error: Extracted date parameter is empty")
		http.Error(w, "date query parameter identifier is mandatory", http.StatusBadRequest)
		return
	}

	logger.Infof("[API Request] Fetching historical orders for date tracking sequence: %s", dateStr)

	// 2. Query historical entries recorded in the SQL Hypertable using indexable trading_date
	dbOrders := make([]models.OrderBookEntry, 0)
	logger.Warnf("Executing DB query on gidh_orders for trading_date: %s", dateStr)

	rows, err := a.pool.Query(r.Context(), `
		SELECT order_id, symbol, side, order_type, quantity, filled_qty, price, status, timestamp, user_email
		FROM gidh_orders
		WHERE trading_date = $1::date
		ORDER BY timestamp DESC`, dateStr)
	if err != nil {
		logger.Errorf("CRITICAL: Database query execution failure for date %s: %v", dateStr, err)
		http.Error(w, "Database query execution failure", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var scanCount int
	for rows.Next() {
		var o models.OrderBookEntry
		var side, oType string
		scanCount++

		err := rows.Scan(&o.OrderID, &o.Symbol, &side, &oType, &o.Qty, &o.FilledQty, &o.Price, &o.Status, &o.Timestamp, &o.UserEmail)
		if err != nil {
			logger.Errorf("Scan Error on row %d for date %s: %v", scanCount, dateStr, err)
			continue
		}
		o.Side = strings.ToUpper(side)
		o.OrderType = strings.ToUpper(oType)
		dbOrders = append(dbOrders, o)
	}

	logger.Infof("Successfully fetched and parsed %d historical orders from database for date %s", len(dbOrders), dateStr)

	// 3. Extract unexecuted/floating orders currently active inside internal engine memory
	orderMap := make(map[string]models.OrderBookEntry)
	for _, o := range dbOrders {
		orderMap[o.OrderID] = o
	}

	// Blend active configurations memory entries on top of historical slices
	if a.OrderManager != nil {
		var memoryOrders []models.OrderBookEntry
		if liveMgr, ok := a.OrderManager.(*order.LiveOrderManager); ok {
			logger.Debug("Blending active LiveOrderManager memory cache states...")
			memoryOrders = liveMgr.GetOrders("")
		} else if paperMgr, ok := a.OrderManager.(*order.PaperPositionManager); ok {
			logger.Debug("Blending active PaperPositionManager memory cache states...")
			memoryOrders = paperMgr.GetOrders("")
		}

		var blendCount int
		for _, mo := range memoryOrders {
			// Memory cache always overrides stale database logs for open entries
			orderMap[mo.OrderID] = mo
			blendCount++
		}
		if blendCount > 0 {
			logger.Infof("Blended %d floating/pending orders from volatile memory into layout stream", blendCount)
		}
	}

	// Flatten the map back into a clean chronological array sequence
	finalOrders := make([]models.OrderBookEntry, 0, len(orderMap))
	for _, o := range orderMap {
		finalOrders = append(finalOrders, o)
	}

	// Sort array descending so newest entry items sit right on top of the UI list view
	sort.Slice(finalOrders, func(i, j int) bool {
		return finalOrders[i].Timestamp.After(finalOrders[j].Timestamp)
	})

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   finalOrders,
	}); err != nil {
		logger.Errorf("Web Server Serializer Error: Failed to flush history payload down network pipeline: %v", err)
	}
}

func (a *App) handleGetHistoricalPositions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	pathOnly := r.URL.Path
	if len(pathOnly) <= len("/api/positions/history/") {
		http.Error(w, "date tracking sequence identifier is required", http.StatusBadRequest)
		return
	}
	dateStr := pathOnly[len("/api/positions/history/"):]
	if idx := strings.Index(dateStr, "?"); idx != -1 {
		dateStr = dateStr[:idx]
	}
	dateStr = strings.TrimSpace(strings.TrimSuffix(dateStr, "/"))

	if dateStr == "" {
		http.Error(w, "date tracking sequence identifier is required", http.StatusBadRequest)
		return
	}

	// 1. Fetch completed/settled historical position frames from the database table
	positionMap := make(map[string]models.Position)
	rows, err := a.pool.Query(r.Context(), `
		SELECT symbol, product, side, net_quantity, avg_price, realized_pnl, target_price, stop_loss_price
		FROM gidh_positions
		WHERE trading_date = $1::date`, dateStr)
	if err != nil {
		logger.Errorf("Failed to query historical positions ledger for date %s: %v", dateStr, err)
		http.Error(w, "Database query execution failure", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var p models.Position
		err := rows.Scan(&p.Symbol, &p.Product, &p.Side, &p.NetQuantity, &p.AveragePrice, &p.RealizedPnL, &p.TargetPrice, &p.StopLossPrice)
		if err != nil {
			logger.Errorf("Failed to scan historical position entry: %v", err)
			continue
		}
		p.Symbol = strings.ToUpper(p.Symbol)
		p.Product = strings.ToUpper(p.Product)
		p.Side = strings.ToUpper(p.Side)

		key := fmt.Sprintf("%s:%s", p.Symbol, p.Product)
		positionMap[key] = p
	}

	// 2. Ingest live exposure slots from RAM memory caches to override active parameters
	if a.OrderManager != nil {
		livePositions := a.OrderManager.GetAllPositions()

		for _, lp := range livePositions {
			lp.Symbol = strings.ToUpper(lp.Symbol)
			lp.Product = strings.ToUpper(lp.Product)
			key := fmt.Sprintf("%s:%s", lp.Symbol, lp.Product)

			// RAM state always dictates the true active net_quantity and current localized boundaries
			positionMap[key] = lp
		}
	}

	// 3. Convert integrated state map back into an unified layout response slice
	finalPositions := make([]models.Position, 0, len(positionMap))
	for _, pos := range positionMap {
		// Clean up fields if position is flat to satisfy squaring off canvas logic
		if pos.NetQuantity == 0 {
			pos.Side = ""
			pos.TargetPrice = 0
			pos.StopLossPrice = 0
			pos.UnrealizedPnL = 0
		}
		finalPositions = append(finalPositions, pos)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status": "success",
		"data":   finalPositions,
	})
}

type StartBacktestRequest struct {
	Date        string   `json:"date"`
	SpeedFactor float64  `json:"speed_factor"`
	Stocks      []string `json:"stocks"`
}

type UpdateSpeedRequest struct {
	SpeedFactor float64 `json:"speed_factor"`
}

type ModifyOrderRequest struct {
	OrderID   string  `json:"order_id"`
	Price     float64 `json:"price"`
	UserEmail string  `json:"user_email"`
}

type CancelOrderRequest struct {
	OrderID   string `json:"order_id"`
	UserEmail string `json:"user_email"`
}

// responseWriter is a wrapper to capture the status code
type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	h, ok := rw.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("webserver does not support hijacking")
	}
	return h.Hijack()
}

func (rw *responseWriter) Flush() {
	if f, ok := rw.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		next.ServeHTTP(wrappedWriter, r)
		logger.Infof("%s %s %d %v", r.Method, r.URL.Path, wrappedWriter.statusCode, time.Since(start))
	})
}
