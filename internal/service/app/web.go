package app

import (
	"encoding/json"
	"fmt"
	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/reader"
	"gidh-backend/internal/service/stream"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger"
	"net/http"
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

	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.Port),
		Handler: mux,
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

// 2. Implement the handleBacktestStop function
func (a *App) handleBacktestStop(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	a.managerMu.Lock()
	defer a.managerMu.Unlock()

	if a.StreamManager != nil {
		logger.Info("Stopping stream manager via API request...")

		// Stop the manager (this cancels context and waits for workers)
		a.StreamManager.Stop()

		// Clear the references so the status API reflects the idle state
		a.StreamManager = nil
		a.activeManager = nil

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "stopped",
			"message": "Stream manager terminated successfully",
		})
	} else {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{
			"status":  "idle",
			"message": "No active stream manager to stop",
		})
	}
}

type StartBacktestRequest struct {
	Date        string   `json:"date"`
	SpeedFactor float64  `json:"speed_factor"`
	Stocks      []string `json:"stocks"`
}
