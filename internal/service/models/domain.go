package models

import (
	"sync"
	"time"
)

// =====================================================================
// 1. SYSTEM & CONFIGURATION
// =====================================================================

type InstrumentConfig struct {
	Token      uint32 `json:"instrument_token"`
	Name       string `json:"stock_name"`
	IsBacktest bool   `json:"is_backtest"`
}

type MarketDNA struct {
	InstrumentToken uint32
	StockName       string
	TradingDate     time.Time
	POC             float64
	VAH             float64
	VAL             float64
	MacroHVNs       []VPExtrema
	MacroLVNs       []VPExtrema
	TimeBuckets     []TimeBucketDNA
}

type TimeBucketDNA struct {
	MinuteIndex int `json:"minute_index"`

	VolumeMean float64 `json:"volume_mean"`
	VolumeStd  float64 `json:"volume_std"`

	RangeMean float64 `json:"range_mean"`
	RangeStd  float64 `json:"range_std"`

	// Optional future extensions
	VolumeP95 float64 `json:"volume_p95,omitempty"`
	RangeP95  float64 `json:"range_p95,omitempty"`
}

// =====================================================================
// 2. RAW MARKET DATA
// =====================================================================

type TickData struct {
	Timestamp          time.Time  `json:"timestamp"`
	InstrumentToken    uint32     `json:"instrument_token"`
	StockName          string     `json:"stock_name"`
	LastPrice          float64    `json:"last_price"`
	LastTradedQuantity int64      `json:"last_traded_quantity"`
	AverageTradedPrice float64    `json:"average_traded_price"`
	CumulativeVolume   int64      `json:"volume_traded"`
	TotalBuyQuantity   int64      `json:"total_buy_quantity"`
	TotalSellQuantity  int64      `json:"total_sell_quantity"`
	Open               float64    `json:"open"`
	High               float64    `json:"high"`
	Low                float64    `json:"low"`
	Close              float64    `json:"close"`
	Change             float64    `json:"change"`
	Depth              OrderDepth `json:"depth"`
}

type OrderDepth struct {
	Buy  []DepthLevel
	Sell []DepthLevel
}

type DepthLevel struct {
	Price    float64 `json:"price"`
	Quantity int64   `json:"quantity"`
	Orders   int     `json:"orders"`
}

// =====================================================================
// 3. PIPELINE TYPES
// =====================================================================

type TradeStats struct {
	// --- Time context ---
	MinuteIndex int
	Timestamp   time.Time

	// --- Rolling candle stats ---
	Volume1m float64
	Range1m  float64

	// --- Session context ---
	SessionVolume   float64
	SessionAvgRange float64

	// --- Normalized features (must match DNA logic) ---
	NormVolume float64
	NormRange  float64

	// --- DNA reference (optional but useful for debugging) ---
	VolumeMean float64
	VolumeStd  float64
	RangeMean  float64
	RangeStd   float64

	// --- Z-scores (core signal inputs) ---
	VolumeZ float64
	RangeZ  float64

	VolEnergy float64
	RngEnergy float64
}

type EnrichedTick struct {
	Raw            TickData
	DNA            *MarketDNA
	Stats          *TradeStats
	TickVolume     int64
	VolProfile     *VolumeProfileInfo
	FullVolProfile *VolumeProfile
}

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

type Bar struct {
	Timestamp       time.Time  `json:"timestamp"`
	InstrumentToken int32      `json:"instrument_token"`
	StockName       string     `json:"stock_name"`
	Timeframe       string     `json:"timeframe"`
	Open            float64    `json:"open"`
	High            float64    `json:"high"`
	Low             float64    `json:"low"`
	Close           float64    `json:"close"`
	Volume          float64    `json:"volume"`
	VWAP            float64    `json:"vwap,omitempty"`
	POC             float64    `json:"poc,omitempty"`
	VAH             float64    `json:"vah,omitempty"`
	VAL             float64    `json:"val,omitempty"`
	VolEnergy       float64    `json:"vol_energy,omitempty"`
	RngEnergy       float64    `json:"rng_energy,omitempty"`
	Ticks           []TickData `json:"ticks,omitempty"`
}
