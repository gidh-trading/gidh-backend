package stream

import (
	"container/heap"
	"context"
	"encoding/csv"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
)

type FileBacktestSource struct {
	dataDir     string
	date        time.Time
	speedFactor float64
	instruments []struct {
		Name  string
		Token uint32
	}
	nameToToken     map[string]uint32
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.RWMutex
	speedUpdateChan chan struct{} // 🟢 Wakes up the sleeping loop immediately on changes
}

type FileBacktestSourceConfig struct {
	DataDir     string
	Date        time.Time
	SpeedFactor float64
	Instruments []struct {
		Name  string
		Token uint32
	}
	NameToToken map[string]uint32
}

// NewFileBacktestSource is the exported constructor for the backtest engine
func NewFileBacktestSource(cfg *FileBacktestSourceConfig) *FileBacktestSource {
	return &FileBacktestSource{
		dataDir:         cfg.DataDir,
		date:            cfg.Date,
		speedFactor:     cfg.SpeedFactor,
		instruments:     cfg.Instruments,
		nameToToken:     cfg.NameToToken,
		speedUpdateChan: make(chan struct{}, 1), // Non-blocking buffer slot
	}
}

func (b *FileBacktestSource) SetSpeedFactor(factor float64) {
	b.mu.Lock()
	b.speedFactor = factor
	b.mu.Unlock()

	// 🟢 Signal to instantly break out of any active sleep loops
	select {
	case b.speedUpdateChan <- struct{}{}:
	default:
	}
}

func (b *FileBacktestSource) GetSpeedFactor() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.speedFactor
}

func (b *FileBacktestSource) ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error {
	h := &tickHeap{}
	heap.Init(h)

	for _, inst := range b.instruments {
		it, err := b.initStreamingIterator(inst.Name)
		if err != nil {
			continue
		}

		if record, err := it.tickReader.Read(); err == nil {
			tick, _ := parseTickRecord(record, it.tickCols, inst.Name, b.nameToToken, 1)
			it.nextDepth, _ = it.readNextDepthSnapshot()
			heap.Push(h, &heapItem{tick: *tick, iterator: it})
		}
	}

	// 🧠 Track active baseline frames
	var anchorMarketTime time.Time
	var anchorRealTime time.Time
	var activeSpeedFactor float64

	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)

		for item.iterator.nextDepth != nil && !item.iterator.nextDepth.timestamp.After(item.tick.Timestamp) {
			item.iterator.currentDepth = item.iterator.nextDepth.depth
			item.iterator.nextDepth, _ = item.iterator.readNextDepthSnapshot()
		}
		item.tick.Depth = item.iterator.currentDepth

		select {
		case tickChan <- item.tick:
		case <-ctx.Done():
			return ctx.Err()
		}

		// ============================================================================
		// DYNAMIC CLOCK RE-ANCHORING MATRIX
		// ============================================================================
		for {
			currentSpeed := b.GetSpeedFactor()
			if currentSpeed == 0 {
				break
			}

			// ⚡ FORCE RE-ANCHOR: If first run OR if the speed factor changed mid-stream
			if anchorMarketTime.IsZero() || currentSpeed != activeSpeedFactor {
				anchorMarketTime = item.tick.Timestamp
				anchorRealTime = time.Now()
				activeSpeedFactor = currentSpeed
				break // Skip sleep for this specific tick to establish new reference frame
			}

			// Calculate duration relative to our dynamic anchor point instead of session start
			marketDuration := item.tick.Timestamp.Sub(anchorMarketTime)
			var simulatedDuration time.Duration

			if currentSpeed == -99 {
				simulatedDuration = marketDuration
			} else if currentSpeed > 0 {
				simulatedDuration = time.Duration(float64(marketDuration) / currentSpeed)
			} else {
				break
			}

			targetRealTime := anchorRealTime.Add(simulatedDuration)
			now := time.Now()
			waitDuration := targetRealTime.Sub(now)

			if waitDuration <= 0 {
				break
			}

			timer.Reset(waitDuration)

			select {
			case <-timer.C:
				// Normal timed wakeup
				break
			case <-b.speedUpdateChan:
				// Interrupted by an API request! Force clean the timer wheel and loop immediately
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue // Triggers the condition check above to immediately re-anchor
			case <-ctx.Done():
				return ctx.Err()
			}
			break
		}

		// Read next tick
		if record, err := item.iterator.tickReader.Read(); err == nil {
			nextTick, _ := parseTickRecord(record, item.iterator.tickCols, item.iterator.stockName, b.nameToToken, 0)
			item.tick = *nextTick
			heap.Push(h, item)
		} else {
			item.iterator.Close()
		}
	}
	return ErrBacktestFinished
}

func (b *FileBacktestSource) initStreamingIterator(stockName string) (*tickIterator, error) {
	dateFolder := b.date.Format("2006-01-02")
	base := filepath.Join(b.dataDir, dateFolder)

	tFile, err := os.Open(filepath.Join(base, "live_ticks", "live_ticks_"+stockName+".csv"))
	if err != nil {
		return nil, err
	}
	tReader := csv.NewReader(tFile)
	tHeader, _ := tReader.Read()
	tCols := make(map[string]int)
	for i, n := range tHeader {
		tCols[n] = i
	}

	dFile, err := os.Open(filepath.Join(base, "live_order_depth", "live_order_depth_"+stockName+".csv"))
	if err != nil {
		tFile.Close()
		return nil, err
	}
	dReader := csv.NewReader(dFile)
	dHeader, _ := dReader.Read()
	dCols := make(map[string]int)
	for i, n := range dHeader {
		dCols[n] = i
	}

	return &tickIterator{
		stockName: stockName, nameToToken: b.nameToToken,
		tickFile: tFile, tickReader: tReader, tickCols: tCols,
		depthFile: dFile, depthReader: dReader, depthCols: dCols,
	}, nil
}

func (b *FileBacktestSource) Connect(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	return nil
}
func (b *FileBacktestSource) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return nil
}
func (b *FileBacktestSource) Type() SourceType           { return SourceBacktest }
func (b *FileBacktestSource) Subscribe(t []uint32) error { return nil }
