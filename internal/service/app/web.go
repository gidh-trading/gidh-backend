package app

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/reader"
	"gidh-backend/internal/service/stream"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
	"net"
	"net/http"
	"sort"
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
	mux.HandleFunc("/api/alerts/", a.handleGetAlerts)

	mux.HandleFunc("/api/positions", a.handleGetPositions)
	mux.HandleFunc("/api/orders/place", a.handleOrderPlace)

	// Order Management Routes
	mux.HandleFunc("/api/orders/modify", a.handleOrderModify)
	mux.HandleFunc("/api/orders/cancel", a.handleOrderCancel)

	// Position Management Routes
	mux.HandleFunc("/api/positions/metadata", a.handlePositionMetadata)
	mux.HandleFunc("/api/positions/exit", a.handlePositionExit)

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
		logger.Warnf("Cleanup failed (continuing anyway): %v", err)
	}

	// 5. Override Config with API params
	a.Config.BacktestDate = req.Date
	a.Config.BacktestSpeedFactor = req.SpeedFactor

	// 6. Reload Market Data & DNA for the specific date
	parsedDate, _ := time.Parse("2006-01-02", req.Date)
	dnaMap, advMap := a.loadMarketData(ctx, parsedDate)

	// 7. RE-INITIALIZE PIPELINE (This resets all internal memory/maps)
	if err := a.initPipeline(ctx, dnaMap, advMap); err != nil {
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

		// 1. Stop the stream reader and workers
		a.StreamManager.Stop()

		// 2. Clear bar/rolling state in the pipeline
		if a.Pipeline != nil {
			a.Pipeline.Reset()
		}

		// 3. Clear the Position Manager (Orders/Positions/Prices)
		if a.OrderManager != nil {
			a.OrderManager.ClearPositions()
		}

		// 4. Clear alert history
		a.alertMu.Lock()
		a.topPlayable = make(map[uint32]models.PlayableAlert)
		a.alertMu.Unlock()

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

// 2. Implement the new handleGetAlerts function with the required response wrapper
func (a *App) handleGetAlerts(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// Extract date from the path (/api/alerts/YYYY-MM-DD)
	// Note: If you're on Go 1.22+, you can use r.PathValue("date")
	// if you register the route as "/api/alerts/{date}"

	a.alertMu.RLock()
	var list []models.PlayableAlert
	for _, alert := range a.topPlayable {
		list = append(list, alert)
	}
	a.alertMu.RUnlock()

	// Sort by EnergyDelta descending
	sort.Slice(list, func(i, j int) bool {
		return list[i].EnergyDelta > list[j].EnergyDelta
	})

	// Wrap the result in a 'data' field to match the UI's 'json.data' expectation
	response := map[string]interface{}{
		"status": "success",
		"data":   list,
	}

	json.NewEncoder(w).Encode(response)
}

func (a *App) handleOrderPlace(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.OrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request", http.StatusBadRequest)
		return
	}

	// a.OrderManager would be an interface initialized in app.go
	id, err := a.OrderManager.PlaceOrder(r.Context(), req)
	if err != nil {
		logger.Errorf("Order Placement Failed: %v | Request: %+v", err, req)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	json.NewEncoder(w).Encode(map[string]string{"order_id": id, "status": "success"})
}

// Add the handler implementation:
func (a *App) handleGetPositions(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	// 1. Fetch positions from the manager
	positions := a.OrderManager.GetAllPositions()

	// 2. Wrap in the 'data' field to match your UI's expectation
	response := map[string]interface{}{
		"status": "success",
		"data":   positions,
	}

	// 3. Send response
	if err := json.NewEncoder(w).Encode(response); err != nil {
		logger.Errorf("Failed to encode positions: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (a *App) handlePositionMetadata(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		return
	}
	var req struct {
		Symbol  string  `json:"symbol"`
		Product string  `json:"product"`
		TP      float64 `json:"target_price"`
		SL      float64 `json:"stop_loss_price"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	err := a.OrderManager.UpdatePositionMetadata(req.Symbol, req.Product, req.TP, req.SL)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.WriteHeader(200)
}

func (a *App) handlePositionExit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		return
	}
	var req struct {
		Symbol  string `json:"symbol"`
		Product string `json:"product"`
		Qty     int    `json:"quantity"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	err := a.OrderManager.ExitPosition(r.Context(), req.Symbol, req.Product, req.Qty)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	w.WriteHeader(200)
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

	// Call the modified OrderManager interface method
	err := a.OrderManager.ModifyOrder(req.OrderID, req.Price, req.TargetPrice, req.StopLossPrice)
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

// handleOrderCancel processes requests to move a pending order to a CANCELLED state.
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

	if req.OrderID == "" {
		http.Error(w, "order_id is required", http.StatusBadRequest)
		return
	}

	// Call the OrderManager to cancel the pending order
	err := a.OrderManager.CancelOrder(req.OrderID)
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

type StartBacktestRequest struct {
	Date        string   `json:"date"`
	SpeedFactor float64  `json:"speed_factor"`
	Stocks      []string `json:"stocks"`
}

type ModifyOrderRequest struct {
	OrderID       string  `json:"order_id"`
	Price         float64 `json:"price"`
	TargetPrice   float64 `json:"target_price"`
	StopLossPrice float64 `json:"stop_loss_price"`
}

type CancelOrderRequest struct {
	OrderID string `json:"order_id"`
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

		// Initialize with 200 as default
		wrappedWriter := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}

		// Call the next handler
		next.ServeHTTP(wrappedWriter, r)

		// Log the results using your structured logger
		duration := time.Since(start)
		logger.Infof("%s %s | Status: %d | Duration: %v",
			r.Method,
			r.URL.Path,
			wrappedWriter.statusCode,
			duration,
		)
	})
}
