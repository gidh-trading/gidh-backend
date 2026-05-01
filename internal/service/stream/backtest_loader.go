package stream

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"gidh-backend/internal/service/models"
)

// readNextDepthSnapshot streams rows and consolidates levels sharing a timestamp
func (it *tickIterator) readNextDepthSnapshot() (*depthSnapshot, error) {
	var currentTS time.Time
	depth := models.OrderDepth{
		Buy:  make([]models.DepthLevel, 0),
		Sell: make([]models.DepthLevel, 0),
	}

	if it.pendingDepthRecord != nil {
		ts, _ := parseTimestamp(it.pendingDepthRecord[it.depthCols["timestamp"]])
		currentTS = ts
		side := strings.ToLower(it.pendingDepthRecord[it.depthCols["side"]])
		price, _ := parseFloat(it.pendingDepthRecord, it.depthCols, "price")
		qty, _ := parseInt(it.pendingDepthRecord, it.depthCols, "quantity")
		orders, _ := parseInt(it.pendingDepthRecord, it.depthCols, "orders")

		level := models.DepthLevel{Price: price, Quantity: qty, Orders: int(orders)}
		if side == "buy" {
			depth.Buy = append(depth.Buy, level)
		} else {
			depth.Sell = append(depth.Sell, level)
		}
		it.pendingDepthRecord = nil
	}

	for {
		record, err := it.depthReader.Read()
		if err == io.EOF {
			if !currentTS.IsZero() {
				return &depthSnapshot{timestamp: currentTS, depth: depth}, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}

		ts, err := parseTimestamp(record[it.depthCols["timestamp"]])
		if err != nil {
			continue
		}

		if !currentTS.IsZero() && !ts.Equal(currentTS) {
			it.pendingDepthRecord = record
			return &depthSnapshot{timestamp: currentTS, depth: depth}, nil
		}

		currentTS = ts
		side := strings.ToLower(record[it.depthCols["side"]])

		// Parse all three fields
		price, _ := parseFloat(record, it.depthCols, "price")
		qty, _ := parseInt(record, it.depthCols, "quantity")
		orders, _ := parseInt(record, it.depthCols, "orders") // -> NEW: Parse orders

		// Add Orders to the struct
		level := models.DepthLevel{
			Price:    price,
			Quantity: qty,
			Orders:   int(orders), // Cast int64 to int
		}

		if side == "buy" {
			depth.Buy = append(depth.Buy, level)
		} else {
			depth.Sell = append(depth.Sell, level)
		}
	}
}

func parseTickRecord(record []string, colIndex map[string]int, stockName string, nameToToken map[string]uint32, recordNum int) (*models.TickData, error) {
	tick := &models.TickData{}
	tsStr := record[colIndex["timestamp"]]
	timestamp, err := parseTimestamp(tsStr)
	if err != nil {
		return nil, err
	}

	tick.Timestamp = timestamp
	tick.StockName = stockName
	if token, found := nameToToken[stockName]; found {
		tick.InstrumentToken = token
	}

	tick.LastPrice, _ = parseFloat(record, colIndex, "last_price")
	tick.LastTradedQuantity, _ = parseInt(record, colIndex, "last_traded_quantity")
	tick.AverageTradedPrice, _ = parseFloat(record, colIndex, "average_traded_price")
	tick.CumulativeVolume, _ = parseInt(record, colIndex, "volume_traded")
	tick.TotalBuyQuantity, _ = parseInt(record, colIndex, "total_buy_quantity")
	tick.TotalSellQuantity, _ = parseInt(record, colIndex, "total_sell_quantity")
	tick.Open, _ = parseFloat(record, colIndex, "ohlc_open")
	tick.High, _ = parseFloat(record, colIndex, "ohlc_high")
	tick.Low, _ = parseFloat(record, colIndex, "ohlc_low")
	tick.Close, _ = parseFloat(record, colIndex, "ohlc_close")
	tick.Change, _ = parseFloat(record, colIndex, "change")
	return tick, nil
}

func parseTimestamp(tsStr string) (time.Time, error) {
	formats := []string{
		"2006-01-02 15:04:05.999999999Z07:00",
		"2006-01-02 15:04:05.999999999+00",
		"2006-01-02 15:04:05.999999999",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, tsStr); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("unable to parse timestamp: %s", tsStr)
}

func parseFloat(record []string, colIndex map[string]int, key string) (float64, error) {
	idx, ok := colIndex[key]
	if !ok || record[idx] == "" {
		return 0, nil
	}
	return strconv.ParseFloat(record[idx], 64)
}

func parseInt(record []string, colIndex map[string]int, key string) (int64, error) {
	idx, ok := colIndex[key]
	if !ok || record[idx] == "" {
		return 0, nil
	}
	if strings.Contains(record[idx], ".") {
		f, _ := strconv.ParseFloat(record[idx], 64)
		return int64(f), nil
	}
	return strconv.ParseInt(record[idx], 10, 64)
}
