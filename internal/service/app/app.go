package app

import (
	"context"
	"fmt"
	"gidh-backend/internal/service/ws"
	"log"
	"net/http"
	"sync"
	"time"

	"gidh-backend/internal/service/db"
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/pipeline"
	"gidh-backend/internal/service/reader"
	"gidh-backend/internal/service/stream"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/config"
	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type App struct {
	Config        *config.Config
	StreamManager *stream.Manager
	Pipeline      *Pipeline
	DBWriter      *writer.DBWriter // Keep a reference to flush on exit

	// State fields
	pool           *pgxpool.Pool
	instrumentList []models.InstrumentConfig
	tokenToName    map[uint32]string
	nameToToken    map[string]uint32

	managerMu     sync.RWMutex
	activePipe    *Pipeline
	activeManager *stream.Manager
}

func NewApp(cfg *config.Config) (*App, error) {
	ctx := context.Background()

	// 1. Initialize Database
	dbURL := cfg.LiveDBURL
	if cfg.Mode == "backtest" {
		dbURL = cfg.BacktestDBURL
	}

	if err := db.InitDB(ctx, dbURL); err != nil {
		logger.Errorf("Database connection failed: %v", err)
	}

	// If in backtest mode and truncation is requested, clean the DB for the target date
	if cfg.Mode == "backtest" && cfg.TruncateBacktestData && cfg.BacktestDate != "" {
		if err := db.CleanupBacktestData(ctx, cfg.BacktestDate); err != nil {
			logger.Errorf("Backtest cleanup failed: %v", err)
		} else {
			logger.Info("Backtest data cleanup completed successfully")
		}
	}

	app := &App{
		Config:      cfg,
		pool:        db.GetPool(),
		tokenToName: make(map[uint32]string),
		nameToToken: make(map[string]uint32),
	}

	var dnaMap map[uint32]*models.MarketDNA

	// 2. Load DB Components
	if app.pool != nil {

		skipPersistence := config.AppConfig.SkipDatabaseInsert

		if config.AppConfig.Mode == "live" {
			logger.Info("Live mode detected: Forcing database persistence (ignoring SKIP_DATABASE_INSERT)")
			skipPersistence = false
		}

		// Initialize the high-speed DB Writer
		app.DBWriter = writer.NewDBWriter(&writer.DBWriterConfig{
			Pool:               app.pool,
			SkipDatabaseInsert: skipPersistence,
		})

		targetDate := time.Now()
		if cfg.Mode == "backtest" && cfg.BacktestDate != "" {
			parsedDate, err := time.Parse("2006-01-02", cfg.BacktestDate)
			if err == nil {
				targetDate = parsedDate
			}
		}

		var err error
		dnaReader := reader.NewDNAReader(app.pool)
		dnaMap, err = dnaReader.FetchMarketDNA(ctx, targetDate)
		if err != nil {
			logger.Errorf("FAILED TO LOAD MARKET DNA for %s: %v", targetDate.Format("2006-01-02"), err)
		} else {
			logger.Infof("Successfully loaded DNA baselines for %d instruments for date %s",
				len(dnaMap), targetDate.Format("2006-01-02"))
		}

		instReader := reader.NewInstrumentReader(app.pool)

		if config.AppConfig.Mode == "live" {
			app.instrumentList, _ = instReader.FetchActiveConfigs(ctx)
		} else {
			app.instrumentList, _ = instReader.FetchBacktestConfigs(ctx)
		}
	}

	if dnaMap == nil {
		dnaMap = make(map[uint32]*models.MarketDNA)
	}

	hub := ws.NewHub()

	go hub.Run()

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		ws.ServeWs(hub, w, r)
	})

	port := fmt.Sprintf(":%s", config.AppConfig.Port)
	log.Printf("Server starting on %s", port)

	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatal("ListenAndServe:", err)
	}

	for _, c := range app.instrumentList {
		app.tokenToName[c.Token] = c.Name
		app.nameToToken[c.Name] = c.Token
	}

	vpStage := pipeline.NewVolumeProfileStage(app.instrumentList, app.pool)

	// Recover today's profiles if starting mid-session
	if config.AppConfig.Mode == "live" {
		if err := vpStage.LoadExistingProfiles(ctx, time.Now()); err != nil {
			logger.Errorf("Failed to load existing volume profiles: %v", err)
		}
	}

	// 3. Initialize Pipeline Stages
	enrichmentStage := pipeline.NewEnrichmentStage(dnaMap)
	barStage := pipeline.NewBarBuilderStage()

	// Inject DBWriter into Pipeline
	app.Pipeline = NewPipeline(vpStage, enrichmentStage, barStage, app.DBWriter)
	app.activePipe = app.Pipeline

	// 4. Initialize Data Stream
	source, err := app.createDataSource()
	if err != nil {
		return nil, err
	}

	app.StreamManager = stream.NewStreamManager(source, app.Pipeline)
	app.activeManager = app.StreamManager

	return app, nil
}

func (a *App) Start(ctx context.Context) error {
	logger.Info("Starting Gidh Trading Backend (Headless Mode)...")

	if err := a.StreamManager.Start(); err != nil {
		logger.Errorf("Failed to start data stream: %v", err)
		return err
	}

	logger.Info("Data stream connected and Pipeline is active.")
	return nil
}

func (a *App) Stop() {
	logger.Info("Shutting down Gidh application...")
	if a.StreamManager != nil {
		a.StreamManager.Stop()
	}

	// Ensure DB Writer flushes the remaining batch to Postgres before exiting
	if a.DBWriter != nil {
		a.DBWriter.Close()
	}

	db.CloseDB()
}
