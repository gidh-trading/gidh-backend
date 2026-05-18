package app

import (
	"context"
	"gidh-backend/internal/service/order"
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
	kiteconnect "github.com/zerodha/gokiteconnect/v4"
)

type App struct {
	Config         *config.Config
	StreamManager  *stream.Manager
	Pipeline       *Pipeline
	DBWriter       *writer.DBWriter
	OrderManager   order.PositionManager
	kiteClient     *kiteconnect.Client
	server         *http.Server
	wsHub          *ws.Hub
	pool           *pgxpool.Pool
	instrumentList []models.InstrumentConfig
	tokenToName    map[uint32]string
	nameToToken    map[string]uint32
	alertMu        sync.RWMutex
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

	app.initKiteClient()

	if err := app.initDatabase(ctx); err != nil {
		return nil, err
	}

	app.initWebServer()

	app.initOrderManager()

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

func (a *App) initOrderManager() {
	if a.Config.Mode == "live" {
		a.OrderManager = order.NewLiveOrderManager(a.kiteClient, a.wsHub, a.DBWriter)
	} else {
		a.OrderManager = order.NewPaperPositionManager(a.wsHub, a.DBWriter)
	}
}

// loadMarketData fetches instrument definitions, DNA baselines, and execution profiles for a target date.
func (a *App) loadMarketData(ctx context.Context, targetDate time.Time) (map[uint32]*models.MarketDNA, map[uint32]*models.InstrumentProfile) {
	if a.pool == nil {
		return make(map[uint32]*models.MarketDNA), make(map[uint32]*models.InstrumentProfile)
	}

	// 1. Load DNA Baselines
	dnaReader := reader.NewDNAReader(a.pool)
	dnaMap, err := dnaReader.FetchMarketDNA(ctx, targetDate)
	if err != nil {
		logger.Errorf("FAILED TO LOAD MARKET DNA for %s: %v", targetDate.Format("2006-01-02"), err)
	}

	// 2. Load Active Instrument Config Mappings based on Mode
	instReader := reader.NewInstrumentReader(a.pool)
	if a.Config.Mode == "live" {
		a.instrumentList, _ = instReader.FetchActiveConfigs(ctx)
	} else {
		a.instrumentList, _ = instReader.FetchBacktestConfigs(ctx)
	}

	// 3. 🔥 Fetch full instrument profile parameters (bucket_size, adv_30d) from DB
	profilesMap, err := instReader.FetchInstrumentProfiles(ctx)
	if err != nil {
		logger.Errorf("FAILED TO LOAD INSTRUMENT PROFILES: %v", err)
		profilesMap = make(map[uint32]*models.InstrumentProfile)
	}

	// Rebuild fast token internal lookups
	a.tokenToName = make(map[uint32]string)
	a.nameToToken = make(map[string]uint32)
	for _, c := range a.instrumentList {
		a.tokenToName[c.Token] = c.Name
		a.nameToToken[c.Name] = c.Token
	}

	return dnaMap, profilesMap
}

func (a *App) initPipeline(ctx context.Context, dnaMap map[uint32]*models.MarketDNA, profilesMap map[uint32]*models.InstrumentProfile) error {

	vpStage := pipeline.NewVolumeProfileStage(a.instrumentList, a.pool, a.wsHub)

	if a.Config.Mode == "live" {
		if err := vpStage.LoadExistingProfiles(ctx, time.Now()); err != nil {
			logger.Errorf("Failed to load existing volume profiles: %v", err)
		}
	}

	advMap := make(map[uint32]float64)
	for token, prof := range profilesMap {
		advMap[token] = float64(prof.ADV30d)
	}

	enrichmentStage := pipeline.NewEnrichmentStage(a.OrderManager, advMap)
	enrichmentStage.UpdateDNAMap(dnaMap)

	analyticsStage := pipeline.NewAnalyticsStage(profilesMap)

	barManager := pipeline.NewBarManager(a.DBWriter, a.wsHub)

	a.Pipeline = NewPipeline(vpStage, enrichmentStage, analyticsStage, barManager, a.DBWriter)
	a.activePipe = a.Pipeline
	return nil
}

func (a *App) initStreamManager() error {
	source, err := a.createDataSource()
	if err != nil {
		return err
	}

	a.StreamManager = stream.NewStreamManager(source, a.Pipeline)
	a.activeManager = a.StreamManager
	return nil
}

func (a *App) initKiteClient() {
	if a.Config.KiteAPIKey != "" {
		a.kiteClient = kiteconnect.New(a.Config.KiteAPIKey)
		a.kiteClient.SetAccessToken(a.Config.KiteAccessToken)
		logger.Info("Kite Connect client initialized")
	}
}

func (a *App) Start(ctx context.Context) error {
	go func() {
		logger.Infof("Server starting on %s", a.server.Addr)
		if err := a.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("Server error: %v", err)
		}
	}()

	if a.StreamManager != nil {
		return a.StreamManager.Start()
	}

	logger.Info("App started in IDLE mode, waiting for backtest start command...")
	return nil
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
	// [Analytic Client close sequence removed from here]
	db.CloseDB()
}
