package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/reader"
	"gidh-backend/internal/service/stream"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/config"
	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	Config         *config.Config
	StreamManager  *stream.Manager
	Pipeline       *Pipeline
	DBWriter       *writer.DBWriter
	server         *http.Server
	wsHub          *ws.Hub
	pool           *pgxpool.Pool
	instrumentList []models.InstrumentConfig
	tokenToName    map[uint32]string
	nameToToken    map[string]uint32
	managerMu      sync.RWMutex
	activePipe     *Pipeline
	activeManager  *stream.Manager
}

// NewApp orchestrates the application setup.
func NewApp(cfg *config.Config) (*App, error) {
	ctx := context.Background()
	app := &App{
		Config:      cfg,
		tokenToName: make(map[uint32]string),
		nameToToken: make(map[string]uint32),
	}

	if err := app.initDatabase(ctx); err != nil {
		return nil, err
	}
	app.initWebServer()

	// If live, we load everything and start immediately.
	// If backtest, we wait for the API call.
	if cfg.Mode == "live" {
		dnaMap, advMap := app.loadMarketData(ctx, time.Now())
		if err := app.initPipeline(ctx, dnaMap, advMap); err != nil {
			return nil, err
		}
		if err := app.initStreamManager(); err != nil {
			return nil, err
		}
	}

	return app, nil
}

// initDatabase handles DB connections and writer initialization.
func (a *App) initDatabase(ctx context.Context) error {
	dbURL := a.Config.LiveDBURL
	if a.Config.Mode == "backtest" {
		dbURL = a.Config.BacktestDBURL
	}

	if err := db.InitDB(ctx, dbURL); err != nil {
		logger.Fatalf("Database connection failed: %v", err)
	}
	a.pool = db.GetPool()

	if a.Config.Mode == "backtest" && a.Config.TruncateBacktestData && a.Config.BacktestDate != "" {
		if err := db.CleanupBacktestData(ctx, a.Config.BacktestDate); err != nil {
			logger.Errorf("Backtest cleanup failed: %v", err)
		}
	}

	// Initialize the high-speed DB Writer
	skipPersistence := a.Config.SkipDatabaseInsert
	if a.Config.Mode == "live" {
		skipPersistence = false
	}

	a.DBWriter = writer.NewDBWriter(&writer.DBWriterConfig{
		Pool:               a.pool,
		SkipDatabaseInsert: skipPersistence,
	})

	return nil
}

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
			"mode":         a.Config.Mode,
			"current_date": currentDate,
			"is_active":    a.StreamManager != nil,
		})
	})
	mux.HandleFunc("/api/backtest/start", a.handleBacktestStart)

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

// loadMarketData fetches instrument and DNA baselines from DB for a specific date.
func (a *App) loadMarketData(ctx context.Context, targetDate time.Time) (map[uint32]*models.MarketDNA, map[uint32]float64) {
	if a.pool == nil {
		return make(map[uint32]*models.MarketDNA), make(map[uint32]float64)
	}

	// 1. Load DNA Baselines for the specific target date
	dnaReader := reader.NewDNAReader(a.pool)
	dnaMap, err := dnaReader.FetchMarketDNA(ctx, targetDate)
	if err != nil {
		logger.Errorf("FAILED TO LOAD MARKET DNA for %s: %v", targetDate.Format("2006-01-02"), err)
	}

	// 2. Load Instruments based on current Mode (Live or Backtest)
	instReader := reader.NewInstrumentReader(a.pool)
	if a.Config.Mode == "live" {
		a.instrumentList, _ = instReader.FetchActiveConfigs(ctx)
	} else {
		// In backtest mode, this will return only the stocks
		// updated via UpdateBacktestSelection in the start handler.
		a.instrumentList, _ = instReader.FetchBacktestConfigs(ctx)
	}

	// 3. Load 30-day Average Daily Volume (ADV) profiles for Z-score normalization
	advMap, err := instReader.FetchADVProfiles(ctx)
	if err != nil {
		logger.Errorf("FAILED TO LOAD ADV PROFILES: %v", err)
	}

	// 4. Reset and rebuild internal token/name mapping maps
	// This ensures only the currently active instruments are in memory.
	a.tokenToName = make(map[uint32]string)
	a.nameToToken = make(map[string]uint32)
	for _, c := range a.instrumentList {
		a.tokenToName[c.Token] = c.Name
		a.nameToToken[c.Name] = c.Token
	}

	return dnaMap, advMap
}

// initPipeline configures the data processing stages[cite: 4].
func (a *App) initPipeline(ctx context.Context, dnaMap map[uint32]*models.MarketDNA, advMap map[uint32]float64) error {
	vpStage := pipeline.NewVolumeProfileStage(a.instrumentList, a.pool)

	if a.Config.Mode == "live" {
		if err := vpStage.LoadExistingProfiles(ctx, time.Now()); err != nil {
			logger.Errorf("Failed to load existing volume profiles: %v", err)
		}
	}

	enrichmentStage := pipeline.NewEnrichmentStage(dnaMap)
	barStage := pipeline.NewBarBuilderStage(a.DBWriter, advMap)

	a.Pipeline = NewPipeline(vpStage, enrichmentStage, barStage, a.DBWriter)
	a.activePipe = a.Pipeline
	return nil
}

// initStreamManager creates the live or backtest data source[cite: 4].
func (a *App) initStreamManager() error {
	source, err := a.createDataSource()
	if err != nil {
		return err
	}

	a.StreamManager = stream.NewStreamManager(source, a.Pipeline)
	a.activeManager = a.StreamManager
	return nil
}

func (a *App) Start(ctx context.Context) error {
	go func() {
		logger.Infof("Server starting on %s", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Server error: %v", err)
		}
	}()

	return a.StreamManager.Start()
}

func (a *App) Stop() {
	logger.Info("Shutting down gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if a.server != nil {
		a.server.Shutdown(shutdownCtx)
	}
	if a.wsHub != nil {
		a.wsHub.Stop()
	}
	if a.StreamManager != nil {
		a.StreamManager.Stop()
	}
	if a.DBWriter != nil {
		a.DBWriter.Close()
	}
	db.CloseDB()
}

type StartBacktestRequest struct {
	Date        string   `json:"date"`
	SpeedFactor float64  `json:"speed_factor"`
	Stocks      []string `json:"stocks"`
}
