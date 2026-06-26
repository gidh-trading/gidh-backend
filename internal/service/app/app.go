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
	Config            *config.Config
	StreamManager     *stream.Manager
	Pipeline          *Pipeline
	DBWriter          *writer.DBWriter
	OrderManager      order.PositionManager
	RiskManager       *risk.RiskManager
	StrategyEngine    *strategy.Engine
	BacktestQueue     []string
	CurrentQueueIndex int
	IsMultiDay        bool
	kiteClient        *kiteconnect.Client
	server            *http.Server
	wsHub             *ws.Hub
	pool              *pgxpool.Pool
	instrumentList    []models.InstrumentConfig
	tokenToName       map[uint32]string
	nameToToken       map[string]uint32
	alertMu           sync.RWMutex
	managerMu         sync.RWMutex
	activePipe        *Pipeline
	activeManager     *stream.Manager
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
		dnaMap, advMap, vwapPercentilesMap := app.loadMarketData(ctx, time.Now())
		if err := app.initPipeline(ctx, dnaMap, advMap, vwapPercentilesMap); err != nil { // ⚡ Updated
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
		a.OrderManager = order.NewLiveOrderManager(a.kiteClient, a.wsHub, a.DBWriter, a.Config.SkipLiveExecution)
	} else {
		a.OrderManager = order.NewPaperPositionManager(a.wsHub, a.DBWriter)
	}
}

// loadMarketData fetches instrument definitions, DNA baselines, execution profiles, and VWAP percentiles for a target date.
func (a *App) loadMarketData(ctx context.Context, targetDate time.Time) (
	map[uint32]*models.MarketDNA,
	map[uint32]*models.InstrumentProfile,
	map[uint32]*models.VWAPDistancePercentile, // ⚡ Added return field
) {
	if a.pool == nil {
		return make(map[uint32]*models.MarketDNA), make(map[uint32]*models.InstrumentProfile), make(map[uint32]*models.VWAPDistancePercentile)
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

	profilesMap, err := instReader.FetchInstrumentProfiles(ctx, targetDate)
	if err != nil {
		logger.Errorf("FAILED TO LOAD INSTRUMENT PROFILES: %v", err)
		profilesMap = make(map[uint32]*models.InstrumentProfile)
	}

	// 3. ⚡ Load VWAP Distance Percentiles for the session
	vpReader := reader.NewVWAPPercentileReader(a.pool)
	vwapPercentilesMap, err := vpReader.FetchVWAPDistancePercentiles(ctx, targetDate)
	if err != nil {
		logger.Errorf("FAILED TO LOAD VWAP DISTANCE PERCENTILES: %v", err)
		vwapPercentilesMap = make(map[uint32]*models.VWAPDistancePercentile)
	}

	// Rebuild fast token internal lookups
	a.tokenToName = make(map[uint32]string)
	a.nameToToken = make(map[string]uint32)
	for _, c := range a.instrumentList {
		a.tokenToName[c.Token] = c.Name
		a.nameToToken[c.Name] = c.Token
	}

	return dnaMap, profilesMap, vwapPercentilesMap
}

func (a *App) initPipeline(
	ctx context.Context,
	dnaMap map[uint32]*models.MarketDNA,
	profilesMap map[uint32]*models.InstrumentProfile,
	vwapPercentilesMap map[uint32]*models.VWAPDistancePercentile,
) error {
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
	barManager := pipeline.NewBarManager(a.wsHub, a.DBWriter, profilesMap, dnaMap)

	scoutStage := pipeline.NewScoutStage(a.wsHub, profilesMap)

	// 5. Assemble the streamlined Execution Pipeline Stage
	a.Pipeline = NewPipeline(vpStage, enrichmentStage, barManager, scoutStage, a.DBWriter)

	// ========================================================================
	// ⚡ MODULAR SEPARATED INTERFACE ASSEMBLY PIPELINE
	// ========================================================================

	logger.Infof("[System Initialization] Assembling Algorithmic Strategy Layer...")

	// Construct the symbol name map for the execution engine registry
	symbolProfiles := make(map[string]*models.InstrumentProfile)
	for _, prof := range profilesMap {
		if prof.StockName != "" {
			symbolProfiles[prof.StockName] = prof
		}
	}

	symbolVwapPercentiles := make(map[string]*models.VWAPDistancePercentile)
	for _, vp := range vwapPercentilesMap {
		if vp.StockName != "" {
			symbolVwapPercentiles[vp.StockName] = vp
		}
	}

	// Step A: Initialize the dynamic Strategy Engine universally across ALL runtime modes
	a.StrategyEngine = strategy.NewEngine(1*time.Hour, symbolProfiles, symbolVwapPercentiles, a.DBWriter)

	// 🔥 Step B: Dynamic Strategy Registry Plug-and-Play Initialization Hook!
	logger.Infof("[System Initialization] Registering multi-strategy algorithms into execution engine matrix...")
	a.StrategyEngine.ActiveRouter.RegisterStrategy(strategy.NewMomentumRunStrategy())

	// Connect macro streaming listeners
	barManager.MacroListener = a.StrategyEngine

	// Step C: Initialize standalone Risk, Capital and Broker Order Controllers
	a.RiskManager = risk.NewRiskManager(a.OrderManager, a.StrategyEngine)

	// Connects decoupled order execution updates straight to the risk management layer.
	if a.OrderManager != nil && a.RiskManager != nil {
		a.OrderManager.RegisterPositionChangeCallback(a.RiskManager.HandleManualAndBrokerStateSync)
		logger.Info("[Startup] Event-Driven Position State Sync Callback registered between Order and Risk layers.")
	}

	a.Pipeline.AlgoAgent = a.RiskManager
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

			// 🔥 NEW: Fetch last known prices from DB to enable instant PnL math
			lastKnownPrices := make(map[string]float64)
			for _, pos := range dbPositions {
				if pos.NetQuantity != 0 {
					var lastPrice float64
					// Query the most recent close price for this symbol before the server shut down
					errQuery := a.pool.QueryRow(ctx, `
						SELECT close 
						FROM gidh_bars 
						WHERE stock_name = $1 
						ORDER BY timestamp DESC 
						LIMIT 1`, pos.Symbol).Scan(&lastPrice)

					if errQuery == nil && lastPrice > 0 {
						lastKnownPrices[pos.Symbol] = lastPrice
					} else {
						// Fallback to AveragePrice so the math doesn't break if no historical bars exist yet
						lastKnownPrices[pos.Symbol] = pos.AveragePrice
					}
				}
			}

			// Inject the prices into the Paper Manager's memory
			paperMgr.SeedLastPrices(lastKnownPrices)

			logger.Info("[Paper Startup] Engine state fully rehydrated from database storage.")
		}
	}
}

func (a *App) runMultiDayBacktestProcessor(req StartMultiDayBacktestRequest) {
	ctx := context.Background()
	logger.Infof("Initializing multi-day backtest execution loop for %d days", len(req.Dates))

	// 1. Initialize global batch state metrics for tracking via /api/backtest/status
	a.managerMu.Lock()
	a.BacktestQueue = req.Dates
	a.IsMultiDay = true
	a.CurrentQueueIndex = 0
	a.managerMu.Unlock()

	// Ensure structural variables are cleaned up when the background processor goroutine exits
	defer func() {
		a.managerMu.Lock()
		a.IsMultiDay = false
		a.BacktestQueue = nil
		a.CurrentQueueIndex = 0
		a.managerMu.Unlock()
		logger.Info("🎉 [Multi-Day] Multi-day background processor goroutine exited safely")
	}()

	for idx, dateStr := range req.Dates {
		logger.Infof("[Multi-Day] Starting Phase %d/%d for Date: %s", idx+1, len(req.Dates), dateStr)

		// 2. Lock mutex to configure application engine dependencies safely
		a.managerMu.Lock()
		a.CurrentQueueIndex = idx // update active status indicator index mid-run

		// Teardown any lingering execution stream frame
		if a.StreamManager != nil {
			logger.Infof("[Multi-Day] Cleaning active stream manager reference before processing date: %s", dateStr)
			a.StreamManager.Stop()
		}

		// 3. Update DB target selection for stocks
		instReader := reader.NewInstrumentReader(a.pool)
		if err := instReader.UpdateBacktestSelection(ctx, req.Stocks); err != nil {
			a.managerMu.Unlock()
			logger.Errorf("[Multi-Day] Failed to update stock selection for %s: %v", dateStr, err)
			return
		}

		// 4. Prepare/Extract File Data (.tar.xz)
		if err := stream.PrepareBacktestData(a.Config.BacktestBackupDir, a.Config.BacktestDataDir, dateStr); err != nil {
			a.managerMu.Unlock()
			logger.Errorf("[Multi-Day] Data preparation missing or extraction failed for %s: %v", dateStr, err)
			return
		}

		// 5. Cleanup DB tables for this specific day to guarantee a fresh slate
		if err := db.CleanupBacktestData(ctx, dateStr); err != nil {
			a.managerMu.Unlock()
			logger.Errorf("[Multi-Day] CRITICAL: DB cleanup failed for %s: %v", dateStr, err)
			return
		}

		// 6. Update internal configurations for this tracking frame
		a.Config.BacktestDate = dateStr
		a.Config.BacktestSpeedFactor = req.SpeedFactor

		// 7. Reload Market Data & DNA structures for the specific date
		parsedDate, _ := time.Parse("2006-01-02", dateStr)
		dnaMap, profilesMap, vwapPercentilesMap := a.loadMarketData(ctx, parsedDate)

		// 8. Reset & Re-initialize Pipeline architecture (wipes localized indicators/RAM maps)
		if err := a.initPipeline(ctx, dnaMap, profilesMap, vwapPercentilesMap); err != nil {
			a.managerMu.Unlock()
			logger.Errorf("[Multi-Day] Pipeline re-init failed for %s: %v", dateStr, err)
			return
		}

		// 9. Re-init Stream Manager with the new DBBacktestSource instance
		if err := a.initStreamManager(); err != nil {
			a.managerMu.Unlock()
			logger.Errorf("[Multi-Day] Stream Manager init failed for %s: %v", dateStr, err)
			return
		}

		// Capture stream manager reference safely inside the locked critical region
		mgr := a.StreamManager
		a.managerMu.Unlock()

		// 10. Execute Stream and explicitly BLOCK until all ticks are fully processed
		logger.Infof("[Multi-Day] Processing stream data loop for date %s...", dateStr)
		if err := mgr.Start(); err != nil {
			logger.Errorf("[Multi-Day] Stream failed to initialize on date %s: %v", dateStr, err)
			return
		}

		// ⚡ FIX: Wait for all background workers (processors + dispatchers) to complete the day's processing
		logger.Infof("[Multi-Day] Waiting for data ingestion to finish for date %s...", dateStr)
		mgr.Wait()
		logger.Infof("[Multi-Day] Day completed successfully for date: %s", dateStr)

		// 11. Post-Day Cleanup & State Resetting before next loop iteration advances
		// ONLY reset memory if there is a subsequent day to process!
		if idx < len(req.Dates)-1 {
			a.managerMu.Lock()
			logger.Infof("[Multi-Day] Performing end-of-day memory resets for %s", dateStr)

			if a.OrderManager != nil {
				logger.Debug("[Multi-Day] Flushing volatile active position matrices from RAM...")
				a.OrderManager.ClearPositions() // Clear open live position frames from cache
			}
			if a.Pipeline != nil {
				logger.Debug("[Multi-Day] Resetting indicators and rolling pipeline analytics state blocks...")
				a.Pipeline.Reset() // Clear indicators, technical metrics, and state mappings
			}

			a.StreamManager = nil
			a.activeManager = nil
			a.managerMu.Unlock()

			// Small cooldown buffer between days to allow the system connection resources to settle
			time.Sleep(200 * time.Millisecond)
		} else {
			// On the final day, preserve everything so the Web API can inspect or stop it manually
			logger.Infof("[Multi-Day] Final date (%s) completed. Retaining RAM state for Web UI inspection.", dateStr)
		}
	}

	logger.Info("🎉 [Multi-Day] All specified backtest dates completed successfully!")
}
