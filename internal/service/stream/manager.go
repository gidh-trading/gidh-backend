package stream

import (
	"context"
	"errors"
	"sync"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

const processorCount = 2 // Match your physical core layout or configuration

type Manager struct {
	source      TickDataSource
	workerChans []chan models.TickData // 🟢 Partitioned channels: one for each specific worker
	workerPool  *sync.WaitGroup
	ctx         context.Context
	cancel      context.CancelFunc
	processor   TickProcessor
	done        chan struct{} // Channel to signal completion
	once        sync.Once     // Ensure done channel is closed only once

	mu          sync.RWMutex
	currentDate string
}

type TickProcessor interface {
	Process(tick models.TickData) error
}

func NewStreamManager(source TickDataSource, processor TickProcessor) *Manager {
	ctx, cancel := context.WithCancel(context.Background())

	// 🟢 Initialize independent buffered worker channels for asset isolation
	chans := make([]chan models.TickData, processorCount)
	for i := 0; i < processorCount; i++ {
		chans[i] = make(chan models.TickData, 5000)
	}

	return &Manager{
		source:      source,
		workerChans: chans,
		workerPool:  &sync.WaitGroup{},
		ctx:         ctx,
		cancel:      cancel,
		processor:   processor,
		done:        make(chan struct{}),
	}
}

func (sm *Manager) Start() error {
	logger.Infof("Starting stream manager with source: %s", sm.source.Type())

	// Connect to source
	if err := sm.source.Connect(sm.ctx); err != nil {
		logger.Errorf("Failed to connect to source: %v", err)
		return err
	}

	// 1. Start the sequential worker processing pools first
	for i := 0; i < processorCount; i++ {
		sm.workerPool.Add(1)
		go sm.runProcessor(i, sm.workerChans[i])
	}

	// 2. Start the central reader dispatcher goroutine
	sm.workerPool.Add(1)
	go sm.runDispatcher()

	// Wait for all workers (dispatcher + processors) to finish, then signal Done
	go func() {
		sm.workerPool.Wait()
		sm.once.Do(func() {
			close(sm.done)
		})
	}()

	logger.Infof("Stream manager started with %d partitioned processors", processorCount)
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

// 🟢 Reads ticks sequentially from the source and routes them deterministically via modulo hashing
func (sm *Manager) runDispatcher() {
	defer sm.workerPool.Done()

	// Ensure all downstream workers get their channels closed when dispatcher exits
	defer func() {
		for _, ch := range sm.workerChans {
			close(ch)
		}
	}()

	logger.Infof("Stream dispatcher started for source: %s", sm.source.Type())

	// Temporary internal channel to read directly out of the stream source
	sourceChan := make(chan models.TickData, 10000)

	// Launch underlying Reader stream loop
	go func() {
		err := sm.source.ReadTicks(sm.ctx, sourceChan)
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
		close(sourceChan)
	}()

	// Read and distribute ticks to the correct worker channel
	for {
		select {
		case tick, ok := <-sourceChan:
			if !ok {
				return
			}

			// ⚡ Deterministic Token Modulo Routing: Guarantees chronological sequence per stock
			workerID := tick.InstrumentToken % uint32(processorCount)

			select {
			case sm.workerChans[workerID] <- tick:
			case <-sm.ctx.Done():
				return
			}
		case <-sm.ctx.Done():
			return
		}
	}
}

func (sm *Manager) runProcessor(processorID int, ch <-chan models.TickData) {
	defer sm.workerPool.Done()

	logger.Infof("Tick processor %d started", processorID)
	var processedCount int

	for {
		select {
		case tick, ok := <-ch:
			if !ok {
				logger.Infof("Tick processor %d stopped after processing %d ticks (channel closed)", processorID, processedCount)
				return
			}

			sm.updateStatus(tick.Timestamp.Format("2006-01-02"))

			if err := sm.processor.Process(tick); err != nil {
				logger.Errorf("Processor %d failed to process tick for %s: %v", processorID, tick.StockName, err)
			}

			processedCount++
			if processedCount%10000 == 0 {
				logger.Infof("Processor %d processed %d ticks", processorID, processedCount)
			}
		case <-sm.ctx.Done():
			// Context canceled via Stop API request!
			// Drain remaining objects from channel quickly without hanging to unlock sm.workerPool.Wait()
			for tick := range ch {
				_ = sm.processor.Process(tick)
			}
			logger.Infof("Tick processor %d halted via cancellation context flag.", processorID)
			return
		}
	}
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

func (sm *Manager) GetSource() TickDataSource {
	return sm.source
}
