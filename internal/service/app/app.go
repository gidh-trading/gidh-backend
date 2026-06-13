package app

import (
	"context"
	"gidh-backend/internal/service/order"
	"gidh-backend/internal/service/risk"
	"gidh-backend/internal/service/strategy"
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
	RiskManager    *risk.RiskManager
	StrategyEngine *strategy.Engine
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

	if cfg.Mode == "live" {
		app.initKiteClient()
	}

	if err := app.initDatabase(ctx); err != nil {
		return nil, err
	}

	app.initWebServer()

	app.initOrderManager()

	if cfg.Mode == "live" {
		app.initLiveState(ctx)
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

	// 1. Build the fast structural maps for historical parameters
	advMap := make(map[uint32]float64)
	bucketSizeMap := make(map[uint32]float64)

	for token, prof := range profilesMap {
		advMap[token] = float64(prof.ADV30d)
		bucketSizeMap[token] = prof.BucketSize
	}

	// 2. Initialize the Volume Profile Stage (it maps wsHub internally for its independent broadcasts)
	vpStage := pipeline.NewVolumeProfileStage(a.instrumentList, bucketSizeMap, a.pool, a.wsHub)

	if a.Config.Mode == "live" {
		if err := vpStage.LoadExistingProfiles(ctx, time.Now()); err != nil {
			logger.Errorf("Failed to load existing volume profiles: %v", err)
		}
	}

	// 3. Initialize the Enrichment Stage mapping positional metrics
	enrichmentStage := pipeline.NewEnrichmentStage(a.OrderManager, dnaMap, profilesMap)

	// 4. Initialize the decoupled Bar Manager
	barManager := pipeline.NewBarManager(a.wsHub, profilesMap, dnaMap)

	scoutStage := pipeline.NewScoutStage(a.wsHub, profilesMap)

	// 5. Assemble the streamlined Execution Pipeline Stage
	a.Pipeline = NewPipeline(vpStage, enrichmentStage, barManager, scoutStage, a.DBWriter)

	// ========================================================================
	// ⚡ MODULAR SEPARATED INTERFACE ASSEMBLY PIPELINE
	// ========================================================================

	logger.Infof("[System Initialization] Modular Algorithmic Strategy Layer. AlgoAgent Online.")
	// Construct the symbol name map for the execution engine registry
	symbolProfiles := make(map[string]*models.InstrumentProfile)
	for _, prof := range profilesMap {
		if prof.StockName != "" {
			symbolProfiles[prof.StockName] = prof
		}
	}

	if a.Config.Mode != "live" {
		// Step C: Initialize your engine using the compiled symbol map
		a.StrategyEngine = strategy.NewEngine(1*time.Hour, symbolProfiles, a.DBWriter, func(log *strategy.OptimizationTradeLog) {
			// 1. Output a visual stream verification log item
			logger.Infof("🎯 OPTIMIZATION LOG | %s | Side: %s | PnL: %.2f INR | Reason: %s | Wick Ratio: %.2f | VWAP Dist: %.4f",
				log.Symbol, log.TradeSide, log.FinalPnLINR, log.ExitReason, log.EntryWickRatio, log.EntryVwapDistance)

			// 2. Write straight down into our persistent TimescaleDB relational logs table
			if a.pool != nil {
				err := db.LogStrategyOptimizationTrade(
					context.Background(),
					a.pool,
					log.Symbol,
					log.StrategyName,
					log.TradeSide,
					log.MinutesSinceOpen,
					log.EntryTimestamp,
					log.EntryPrice,
					log.EntryVwap,
					log.EntryVolumeRank,
					log.EntryPriceRank,
					log.EntryWickRatio,
					log.EntryVwapDistance,
					log.ExitTimestamp,
					log.ExitPrice,
					log.ExitReason,
					log.FinalPnLINR,
					log.PeakPnLINR,
				)
				if err != nil {
					logger.Errorf("Failed to persist strategy optimization metrics chunk for %s: %v", log.Symbol, err)
				}
			}
		})

		// Connect macro streaming listeners
		barManager.MacroListener = a.StrategyEngine

		// Initialize standalone Risk, Capital and Broker Order Controllers
		moneyManager := risk.NewRiskManager(a.OrderManager, a.StrategyEngine)
		a.Pipeline.AlgoAgent = moneyManager
		a.RiskManager = moneyManager
	} else {
		logger.Infof("[System Initialization] Operating in %s mode. Algorithmic Agent deactivated.", a.Config.Mode)
	}

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

	if a.Config.Mode == "live" {
		if err := a.WarmupEngineState(ctx); err != nil {
			logger.Errorf("Critical execution halt: Warmup engine layer failed: %v", err)
			return err
		}
	}

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

func (a *App) initLiveState(ctx context.Context) {
	if a.Config.Mode != "live" || a.OrderManager == nil {
		return
	}

	logger.Infof("[Startup] Initiating system crash-recovery and state reconstitution sequence...")

	// 1. Recover Session Snapshot from the Local Core Database Tables
	// This pulls today's baseline order audit log and active metadata risk positions
	dbOrders, dbPositions, err := db.LoadSessionSnapshotFromDB(ctx, a.pool)
	if err != nil {
		logger.Errorf("[Startup] CRITICAL Recovery Deficit: Could not load snapshot from DB hypertables: %v", err)
		// Do not return; try to proceed with exchange sync as a emergency fallback
	}

	// Cast the PositionManager interface to its concrete implementation types to inject data
	if liveMgr, ok := a.OrderManager.(*order.LiveOrderManager); ok {
		// 2. Hydrate internal RAM matrices using database historical snapshots
		if err == nil {
			liveMgr.ReconstituteState(dbOrders, dbPositions)
			logger.Info("[Startup] Local RAM memory buffers successfully rehydrated from database ledger.")
		}

		// 3. Perform Live Broker Exchange Audit Verification Checklist
		// Reconciles matching executions or manual user interventions done while server was offline
		logger.Info("[Startup] Synchronizing state directly with broker exchange books...")
		if err := liveMgr.SyncExchangeState(ctx); err != nil {
			logger.Errorf("[Startup] Broker synchronization failure: %v", err)
		} else {
			logger.Info("[Startup] Exchange state audit complete. Local memory is perfectly calibrated.")
		}
	} else if paperMgr, ok := a.OrderManager.(*order.PaperPositionManager); ok {
		// Paper mode doesn't connect to an external broker, so it relies entirely on local storage rehydration
		if err == nil {
			paperMgr.ReconstituteState(dbOrders, dbPositions)
			logger.Info("[Paper Startup] Engine state fully rehydrated from database storage.")
		}
	}
}
