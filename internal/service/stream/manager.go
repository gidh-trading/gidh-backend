package stream

import (
	"context"
	"errors"
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

type Manager struct {
	source     TickDataSource
	tickChan   chan models.TickData
	workerPool *sync.WaitGroup
	ctx        context.Context
	cancel     context.CancelFunc
	processor  TickProcessor
	done       chan struct{} // Channel to signal completion
	once       sync.Once     // Ensure done channel is closed only once

	mu          sync.RWMutex
	currentDate string
}

type TickProcessor interface {
	Process(tick models.TickData) error
}

func NewStreamManager(source TickDataSource, processor TickProcessor) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		source:     source,
		tickChan:   make(chan models.TickData, 10000),
		workerPool: &sync.WaitGroup{},
		ctx:        ctx,
		cancel:     cancel,
		processor:  processor,
		done:       make(chan struct{}),
	}
}

func (sm *Manager) Start() error {
	logger.Infof("Starting stream manager with source: %s", sm.source.Type())

	// Connect to source
	if err := sm.source.Connect(sm.ctx); err != nil {
		logger.Errorf("Failed to connect to source: %v", err)
		return err
	}

	// Start reader goroutine
	sm.workerPool.Add(1)
	go sm.runReader()

	// Start processor goroutines
	processorCount := 20 // Could be made configurable
	for i := 0; i < processorCount; i++ {
		sm.workerPool.Add(1)
		go sm.runProcessor(i)
	}

	// Wait for all workers (reader + processors) to finish, then signal Done
	go func() {
		sm.workerPool.Wait()
		sm.once.Do(func() {
			close(sm.done)
		})
	}()

	logger.Infof("Stream manager started with %d processors", processorCount)
	return nil
}

func (sm *Manager) GetStatus() string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.currentDate
}

func (sm *Manager) updateStatus(date string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.currentDate = date
}

func (sm *Manager) runReader() {
	defer sm.workerPool.Done()
	defer close(sm.tickChan)

	logger.Infof("Stream reader started for source: %s", sm.source.Type())

	err := sm.source.ReadTicks(sm.ctx, sm.tickChan)

	switch {
	case err == nil:
		logger.Info("Stream reader finished normally")
	case errors.Is(err, context.Canceled):
		logger.Info("Stream reader cancelled")
	case errors.Is(err, ErrBacktestFinished):
		logger.Info("Backtest completed successfully")
	default:
		logger.Errorf("Stream reader error: %v", err)
	}
}

func (sm *Manager) runProcessor(processorID int) {
	defer sm.workerPool.Done()

	logger.Infof("Tick processor %d started", processorID)

	var processedCount int
	for tick := range sm.tickChan {

		sm.updateStatus(tick.Timestamp.Format("2006-01-02"))

		if err := sm.processor.Process(tick); err != nil {
			logger.Errorf("Processor %d failed to process tick for %s: %v",
				processorID, tick.StockName, err)
		}

		processedCount++
		if processedCount%10000 == 0 {
			logger.Infof("Processor %d processed %d ticks", processorID, processedCount)
		}
	}

	logger.Infof("Tick processor %d stopped after processing %d ticks",
		processorID, processedCount)
}

func (sm *Manager) Stop() {
	logger.Info("Stopping stream manager")
	sm.cancel()
	sm.workerPool.Wait()

	if err := sm.source.Close(); err != nil {
		logger.Errorf("Failed to close source: %v", err)
	}

	logger.Info("Stream manager stopped")
}

// Done returns a channel that signals when the stream manager has completed
// (only applicable for backtest mode where completion is expected)
func (sm *Manager) Done() <-chan struct{} {
	return sm.done
}

// Wait blocks until the stream manager has completed
func (sm *Manager) Wait() {
	sm.workerPool.Wait()
}
