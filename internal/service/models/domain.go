// internal/service/models/domain.go

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

type InstrumentProfile struct {
	InstrumentToken uint32  `json:"instrument_token"`
	BucketSize      float64 `json:"bucket_size"`
	ATR14           float64 `json:"atr_14"`
	ADRPct          float64 `json:"adr_pct"`
	ADV30d          int64   `json:"adv_30d"`
	ADVVal30d       float64 `json:"adv_val_30d"`
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

type WindowTick struct {
	Price  float64
	Volume float64
}

type EnrichedTick struct {
	Raw            TickData
	TickVolume     int64
	VolProfile     *VolumeProfileInfo
	FullVolProfile *VolumeProfile
	Microstructure TickMicrostructure // Just Buy/Sell now
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
	Timestamp       time.Time `json:"timestamp"`
	InstrumentToken int32     `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	Timeframe       string    `json:"timeframe"`

	// ---- OHLC ----
	Open  float64 `json:"open"`
	High  float64 `json:"high"`
	Low   float64 `json:"low"`
	Close float64 `json:"close"`

	// ---- Volume ----
	Volume float64 `json:"volume"`

	// ---- Optional Auction Metrics ----
	VWAP float64 `json:"vwap"`
	POC  float64 `json:"poc"`
	VAH  float64 `json:"vah"`
	VAL  float64 `json:"val"`

	// ---- UI Only Metrics (Not persisted in DB) ----
	UnrealizedPnL float64 `json:"unrealized_pnl"`
	TotalBuyQty   float64 `json:"total_buy_qty"`
	TotalSellQty  float64 `json:"total_sell_qty"`

	// ---- Microstructure Analytics Heatmap ----
	Heatmap []UIHeatmapCell `json:"heatmap"`

	// ---- Raw ticks ----
	Ticks []TickData `json:"-"`
}

type PlayableAlert struct {
	Timestamp   time.Time `json:"timestamp"`
	StockName   string    `json:"stock_name"`
	Token       uint32    `json:"token"`
	LastPrice   float64   `json:"last_price"`
	EnergyDelta float64   `json:"energy_delta"`
	TotalEnergy float64   `json:"total_energy"`
	BuyEnergy   float64   `json:"buy_energy"`
	SellEnergy  float64   `json:"sell_energy"`
	Timeframe   string    `json:"timeframe"`
}

type AnomalyGridRecord struct {
	TimeBin         time.Time
	InstrumentToken uint32
	PriceBin        float64
	BuyVolume       int64
	SellVolume      int64
	TotalVolume     int64
	PeakZScore      float64
	TickCount       int32
	ClusterVWAP     float64
}

type WhaleBlockRecord struct {
	Timestamp       time.Time
	InstrumentToken uint32
	Price           float64
	Volume          int64
	Side            string
	VExpected       float64
}
