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

type BacktestSource struct {
	dataDir     string
	date        time.Time
	speedFactor float64
	instruments []struct {
		Name  string
		Token uint32
	}
	nameToToken map[string]uint32
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.RWMutex
}

type BacktestSourceConfig struct {
	DataDir     string
	Date        time.Time
	SpeedFactor float64
	Instruments []struct {
		Name  string
		Token uint32
	}
	NameToToken map[string]uint32
}

// NewBacktestSource is the exported constructor for the backtest engine
func NewBacktestSource(cfg *BacktestSourceConfig) *BacktestSource {
	return &BacktestSource{
		dataDir:     cfg.DataDir,
		date:        cfg.Date,
		speedFactor: cfg.SpeedFactor,
		instruments: cfg.Instruments,
		nameToToken: cfg.NameToToken,
	}
}

func (b *BacktestSource) ReadTicks(ctx context.Context, tickChan chan<- models.TickData) error {
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

	var firstTickTime time.Time
	var realStartTime time.Time

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)

		// Sync Depth to Tick
		for item.iterator.nextDepth != nil && !item.iterator.nextDepth.timestamp.After(item.tick.Timestamp) {
			item.iterator.currentDepth = item.iterator.nextDepth.depth
			item.iterator.nextDepth, _ = item.iterator.readNextDepthSnapshot()
		}
		item.tick.Depth = item.iterator.currentDepth

		// BACKPRESSURE
		select {
		case tickChan <- item.tick:
		case <-ctx.Done():
			return ctx.Err()
		}

		// ---> UPDATE SLEEP LOGIC: Track against real elapsed time <---
		if b.speedFactor > 0 {
			if firstTickTime.IsZero() {
				firstTickTime = item.tick.Timestamp
				realStartTime = time.Now()
			} else {
				// Calculate how much market time has passed since the backtest started
				marketDuration := item.tick.Timestamp.Sub(firstTickTime)
				// Calculate how much real-world time SHOULD have passed at 5x speed
				simulatedDuration := time.Duration(float64(marketDuration) / b.speedFactor)

				targetRealTime := realStartTime.Add(simulatedDuration)
				now := time.Now()

				// Only sleep if we are genuinely running ahead of the target schedule
				if targetRealTime.After(now) {
					time.Sleep(targetRealTime.Sub(now))
				}
			}
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

func (b *BacktestSource) initStreamingIterator(stockName string) (*tickIterator, error) {
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

func (b *BacktestSource) Connect(ctx context.Context) error {
	b.ctx, b.cancel = context.WithCancel(ctx)
	return nil
}
func (b *BacktestSource) Close() error {
	if b.cancel != nil {
		b.cancel()
	}
	return nil
}
func (b *BacktestSource) Type() SourceType           { return SourceBacktest }
func (b *BacktestSource) Subscribe(t []uint32) error { return nil }
