package stream

import (
	"encoding/csv"
	"gidh-backend/internal/service/models"
	"os"
	"time"
)

type depthSnapshot struct {
	timestamp time.Time
	depth     models.OrderDepth
}

// tickIterator manages the dual-stream state for a single instrument.
type tickIterator struct {
	stockName   string
	nameToToken map[string]uint32

	// Tick Stream
	tickFile   *os.File
	tickReader *csv.Reader
	tickCols   map[string]int

	// Depth Stream
	depthFile   *os.File
	depthReader *csv.Reader
	depthCols   map[string]int

	// State for synchronization
	currentDepth models.OrderDepth
	nextDepth    *depthSnapshot

	pendingDepthRecord []string
}

func (it *tickIterator) Close() {
	if it.tickFile != nil {
		it.tickFile.Close()
	}
	if it.depthFile != nil {
		it.depthFile.Close()
	}
}
