package models

import (
	"sync"
	"time"
)

// VPNode represents a single price bucket and its accumulated volume.
type VPNode struct {
	Price  float64 `json:"price"`
	Volume int64   `json:"volume"`
}

// VPExtrema represents a detected High Volume Node (Peak) or Low Volume Node (Valley).
type VPExtrema struct {
	Price    float64 `json:"price"`
	Volume   int64   `json:"volume"`
	Strength float64 `json:"strength"`
}

type VolumeProfileInfo struct {
	POC float64 `json:"poc"`
	VAH float64 `json:"vah"`
	VAL float64 `json:"val"`
}

// VolumeProfile tracks the live intraday auction for a single instrument.
type VolumeProfile struct {
	Mu              sync.RWMutex `json:"-"`
	StockName       string       `json:"stock_name"`
	InstrumentToken uint32       `json:"instrument_token"`
	TradingDate     time.Time    `json:"trading_date"`
	BucketSize      float64      `json:"bucket_size"`

	TotalVolume int64 `json:"total_volume"`
	TickCount   int64 `json:"tick_count"`

	// Auction Levels
	POC float64 `json:"poc"`
	VAH float64 `json:"vah"`
	VAL float64 `json:"val"`

	// Fast lookup maps for live tick aggregation
	Buckets      map[float64]int64 `json:"-"`
	SortedPrices []float64         `json:"-"`

	// Slices structured for DB persistence and UI broadcasting
	Nodes []VPNode    `json:"nodes"`
	HVNs  []VPExtrema `json:"hvns"`
	LVNs  []VPExtrema `json:"lvns"`
}

// Copy creates a safe snapshot for asynchronous database writes without locking the main thread.
func (vp *VolumeProfile) Copy() *VolumeProfile {
	clone := &VolumeProfile{
		StockName:       vp.StockName,
		InstrumentToken: vp.InstrumentToken,
		TradingDate:     vp.TradingDate,
		BucketSize:      vp.BucketSize,
		TotalVolume:     vp.TotalVolume,
		TickCount:       vp.TickCount,
		POC:             vp.POC,
		VAH:             vp.VAH,
		VAL:             vp.VAL,
		Buckets:         make(map[float64]int64, len(vp.Buckets)),
		SortedPrices:    make([]float64, len(vp.SortedPrices)),
		Nodes:           make([]VPNode, len(vp.Nodes)),
		HVNs:            make([]VPExtrema, len(vp.HVNs)),
		LVNs:            make([]VPExtrema, len(vp.LVNs)),
	}

	for k, v := range vp.Buckets {
		clone.Buckets[k] = v
	}
	copy(clone.SortedPrices, vp.SortedPrices)
	copy(clone.Nodes, vp.Nodes)
	copy(clone.HVNs, vp.HVNs)
	copy(clone.LVNs, vp.LVNs)

	return clone
}
