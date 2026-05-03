package app

import (
	"context"
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

// NewApp orchestrates the application setup by calling specialized init methods.
func NewApp(cfg *config.Config) (*App, error) {
	ctx := context.Background()

	app := &App{
		Config:      cfg,
		tokenToName: make(map[uint32]string),
		nameToToken: make(map[string]uint32),
	}

	// 1. Infrastructure Setup
	if err := app.initDatabase(ctx); err != nil {
		return nil, err
	}
	app.initWebServer()

	// 2. Data & Domain Setup
	dnaMap := app.loadMarketData(ctx)
	if err := app.initPipeline(ctx, dnaMap); err != nil {
		return nil, err
	}

	// 3. Stream Setup
	if err := app.initStreamManager(); err != nil {
		return nil, err
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

	a.server = &http.Server{
		Addr:    fmt.Sprintf(":%s", a.Config.Port),
		Handler: mux,
	}
}

// loadMarketData fetches instrument and DNA baselines from DB[cite: 4].
func (a *App) loadMarketData(ctx context.Context) map[uint32]*models.MarketDNA {
	if a.pool == nil {
		return make(map[uint32]*models.MarketDNA)
	}

	targetDate := time.Now()
	if a.Config.Mode == "backtest" && a.Config.BacktestDate != "" {
		if parsed, err := time.Parse("2006-01-02", a.Config.BacktestDate); err == nil {
			targetDate = parsed
		}
	}

	// Load DNA Baselines
	dnaReader := reader.NewDNAReader(a.pool)
	dnaMap, err := dnaReader.FetchMarketDNA(ctx, targetDate)
	if err != nil {
		logger.Errorf("FAILED TO LOAD MARKET DNA: %v", err)
	}

	// Load Instruments
	instReader := reader.NewInstrumentReader(a.pool)
	if a.Config.Mode == "live" {
		a.instrumentList, _ = instReader.FetchActiveConfigs(ctx)
	} else {
		a.instrumentList, _ = instReader.FetchBacktestConfigs(ctx)
	}

	for _, c := range a.instrumentList {
		a.tokenToName[c.Token] = c.Name
		a.nameToToken[c.Name] = c.Token
	}

	return dnaMap
}

// initPipeline configures the data processing stages[cite: 4].
func (a *App) initPipeline(ctx context.Context, dnaMap map[uint32]*models.MarketDNA) error {
	vpStage := pipeline.NewVolumeProfileStage(a.instrumentList, a.pool)

	if a.Config.Mode == "live" {
		if err := vpStage.LoadExistingProfiles(ctx, time.Now()); err != nil {
			logger.Errorf("Failed to load existing volume profiles: %v", err)
		}
	}

	enrichmentStage := pipeline.NewEnrichmentStage(dnaMap)
	barStage := pipeline.NewBarBuilderStage(a.DBWriter)

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
