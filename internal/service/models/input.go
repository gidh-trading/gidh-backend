package models

import "time"

// =====================================================================
// RAW MARKET DATA
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
