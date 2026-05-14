package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/internal/service/ws"
	"math"
	"sync"
	"time"
)

func (s *SessionState) Update(vol, rng float64) {
	s.TotalVolume += vol
	s.TotalRange += rng
	s.Count++
}

func (s *SessionState) AvgRange() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.TotalRange / float64(s.Count)
}

type BarBuilderStage struct {
	loc         *time.Location
	rolling     map[uint32]*RollingState
	bar1m       map[uint32]*models.Bar
	bar5m       map[uint32]*models.Bar
	session     map[uint32]*SessionState
	adv30dMap   map[uint32]float64
	lastTs      map[uint32]time.Time
	alertStates map[uint32]string
	onAlert     func(models.PlayableAlert)
	mu          sync.RWMutex
	writer      *writer.DBWriter
	wsHub       *ws.Hub
}

func NewBarBuilderStage(
	w *writer.DBWriter,
	advMap map[uint32]float64,
	hub *ws.Hub,
	onAlert func(models.PlayableAlert),
) *BarBuilderStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &BarBuilderStage{
		loc:         loc,
		rolling:     make(map[uint32]*RollingState),
		bar1m:       make(map[uint32]*models.Bar),
		bar5m:       make(map[uint32]*models.Bar),
		session:     make(map[uint32]*SessionState),
		adv30dMap:   advMap,
		lastTs:      make(map[uint32]time.Time),
		alertStates: make(map[uint32]string),
		onAlert:     onAlert,
		writer:      w,
		wsHub:       hub,
	}
}

func (s *BarBuilderStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(s.loc)
	name := tick.Raw.StockName

	if tick.DNA == nil {
		return nil
	}

	// -------------------------
	// INIT
	// -------------------------
	if s.rolling[token] == nil {
		s.rolling[token] = &RollingState{}
	}
	if s.bar1m[token] == nil {
		s.bar1m[token] = newBar(ts, price, token, name, "1m")
	}
	if s.bar5m[token] == nil {
		s.bar5m[token] = newBar(ts, price, token, name, "5m")
	}
	if s.session[token] == nil {
		s.session[token] = &SessionState{}
	}

	r := s.rolling[token]
	session := s.session[token]

	prevPrice := r.Close
	if prevPrice == 0 {
		prevPrice = price
	}

	// -------------------------
	// 1. ROLLING WINDOW UPDATE (1 Min)
	// -------------------------
	entry := rollingEntry{ts: ts, price: price, vol: vol}
	r.queue = append(r.queue, entry)
	r.Volume += vol

	if len(r.queue) == 1 {
		r.Open = price
		r.High = price
		r.Low = price
	}

	if price > r.High {
		r.High = price
	}
	if price < r.Low {
		r.Low = price
	}
	r.Close = price

	// remove entries older than 60 seconds
	cutoff := ts.Add(-60 * time.Second)
	for len(r.queue) > 0 && r.queue[0].ts.Before(cutoff) {
		old := r.queue[0]
		r.queue = r.queue[1:]
		r.Volume -= old.vol
	}

	// recompute high/low
	if len(r.queue) > 0 {
		r.Open = r.queue[0].price
		r.High = r.queue[0].price
		r.Low = r.queue[0].price

		for _, e := range r.queue {
			if e.price > r.High {
				r.High = e.price
			}
			if e.price < r.Low {
				r.Low = e.price
			}
		}
	}

	// -------------------------
	// 2. UPDATE 1M BAR
	// -------------------------
	b1 := s.bar1m[token]
	expectedTs := ts.Truncate(time.Minute)

	if expectedTs.After(b1.Timestamp) {
		session.Update(b1.Volume, b1.High-b1.Low)

		if s.writer != nil {
			s.writer.AddBar(*b1)
		}

		s.bar1m[token] = newBar(ts, price, token, name, "1m")
		b1 = s.bar1m[token]
	}

	if !expectedTs.Before(b1.Timestamp) {
		updateBar(b1, price, vol)
		b1.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b1.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b1.Ticks = append(b1.Ticks, tick.Raw)
		b1.VWAP = tick.Raw.AverageTradedPrice // Day's VWAP from Kite
		if tick.VolProfile != nil {
			b1.POC = tick.VolProfile.POC
			b1.VAH = tick.VolProfile.VAH
			b1.VAL = tick.VolProfile.VAL
		}
	}

	// -------------------------
	// 3. UPDATE 5M BAR
	// -------------------------
	b5 := s.bar5m[token]
	expected5mTs := ts.Truncate(5 * time.Minute)

	if expected5mTs.After(b5.Timestamp) {
		if s.writer != nil {
			s.writer.AddBar(*b5)
		}

		s.bar5m[token] = newBar(ts, price, token, name, "5m")
		b5 = s.bar5m[token]
	}

	if !expected5mTs.Before(b5.Timestamp) {
		updateBar(b5, price, vol)
		b5.TotalBuyQty = float64(tick.Raw.TotalBuyQuantity)
		b5.TotalSellQty = float64(tick.Raw.TotalSellQuantity)
		b5.VWAP = tick.Raw.AverageTradedPrice
		if tick.VolProfile != nil {
			b5.POC = tick.VolProfile.POC
			b5.VAH = tick.VolProfile.VAH
			b5.VAL = tick.VolProfile.VAL
		}
	}

	// -------------------------
	// 4. TICK DIRECTION & RAW ACCUMULATION (Lee-Ready Algorithm)
	// -------------------------
	dir := 0

	// Step 1: Quote Test via Midpoint
	if len(tick.Raw.Depth.Buy) > 0 && len(tick.Raw.Depth.Sell) > 0 {
		bestBid := tick.Raw.Depth.Buy[0].Price
		bestAsk := tick.Raw.Depth.Sell[0].Price
		midpoint := (bestBid + bestAsk) / 2.0

		// Use a tiny epsilon to handle floating point precision issues at the exact midpoint
		const epsilon = 1e-6

		if price > midpoint+epsilon {
			// Trade occurred closer to the Ask -> Aggressive Buy
			dir = 1
		} else if price < midpoint-epsilon {
			// Trade occurred closer to the Bid -> Aggressive Sell
			dir = -1
		}
	}

	// Step 2: Tick Test Fallback
	// Executes if trade is exactly at the midpoint, or if Level 2 depth is missing
	if dir == 0 {
		if price > prevPrice {
			dir = 1
		} else if price < prevPrice {
			dir = -1
		} else {
			// Zero-Tick: Price hasn't changed. Inherit the dominant flow.
			dir = r.LastDir
		}
	}

	// Update the rolling state with the resolved direction
	if dir != 0 {
		r.LastDir = dir
	}

	tickRange := math.Abs(price - prevPrice)

	// Accumulate pure raw shares/range
	if dir > 0 {
		b1.BuyVolume += vol
		b1.BuyRange += tickRange

		b5.BuyVolume += vol
		b5.BuyRange += tickRange
	} else if dir < 0 {
		b1.SellVolume += vol
		b1.SellRange += tickRange

		b5.SellVolume += vol
		b5.SellRange += tickRange
	}

	// -------------------------
	// 5. NORMALIZATION & DNA Z-SCORE
	// -------------------------
	range1m := r.High - r.Low

	adv30d, ok := s.adv30dMap[token]
	if !ok || adv30d == 0 {
		// Fallback to a default or skip if no profile exists
		adv30d = 450000.0
	}

	// We use the rolling volume here so the Z-score is always a true "last 60 seconds" snapshot
	normVol := r.Volume / adv30d

	normRange := 0.0
	if session.AvgRange() > 0 {
		normRange = range1m / session.AvgRange()
	}

	marketOpen := time.Date(ts.Year(), ts.Month(), ts.Day(), 9, 15, 0, 0, s.loc)
	minuteIndex := int(ts.Sub(marketOpen).Minutes())

	if minuteIndex < 0 || minuteIndex >= len(tick.DNA.TimeBuckets) {
		return nil // Outside market hours
	}

	bucket := tick.DNA.TimeBuckets[minuteIndex]

	// Calculate Raw Z-Scores
	var volumeZ float64
	if bucket.VolumeStd > 1e-6 {
		volumeZ = (normVol - bucket.VolumeMean) / bucket.VolumeStd
	}

	var rangeZ float64
	if bucket.RangeStd > 1e-6 {
		rangeZ = (normRange - bucket.RangeMean) / bucket.RangeStd
	}

	// Below-average volume should mean 0 energy, not negative energy.
	volumeZ = math.Max(0, volumeZ)
	rangeZ = math.Max(0, rangeZ)

	// -------------------------
	// 6. DISTRIBUTE ENERGY TO BUCKETS (The Ratio Method)
	// -------------------------

	// OVERWRITE the Total Energy with the latest true Z-score (Do NOT use +=)
	b1.TotalVolEnergy = volumeZ
	b1.TotalRngEnergy = rangeZ

	b5.TotalVolEnergy = volumeZ
	b5.TotalRngEnergy = rangeZ

	// 1M BAR: Split total energy into Buy/Sell using the actual raw volume ratios
	if b1.Volume > 0 {
		buyRatio := b1.BuyVolume / b1.Volume
		sellRatio := b1.SellVolume / b1.Volume

		b1.BuyVolEnergy = b1.TotalVolEnergy * buyRatio
		b1.SellVolEnergy = b1.TotalVolEnergy * sellRatio
	}

	totalRawRange1m := b1.BuyRange + b1.SellRange
	if totalRawRange1m > 0 {
		buyRngRatio := b1.BuyRange / totalRawRange1m
		sellRngRatio := b1.SellRange / totalRawRange1m

		b1.BuyRngEnergy = b1.TotalRngEnergy * buyRngRatio
		b1.SellRngEnergy = b1.TotalRngEnergy * sellRngRatio
	}

	if s.wsHub != nil {
		key1m := tick.Raw.StockName + ":1m"
		s.wsHub.BroadcastJSON(key1m, map[string]any{
			"type": "bar",
			"data": b1,
		})
	}

	// 5M BAR: Split total energy into Buy/Sell using 5m raw volume ratios
	if b5.Volume > 0 {
		buyRatio5 := b5.BuyVolume / b5.Volume
		sellRatio5 := b5.SellVolume / b5.Volume

		b5.BuyVolEnergy = b5.TotalVolEnergy * buyRatio5
		b5.SellVolEnergy = b5.TotalVolEnergy * sellRatio5
	}

	totalRawRange5m := b5.BuyRange + b5.SellRange
	if totalRawRange5m > 0 {
		buyRngRatio5 := b5.BuyRange / totalRawRange5m
		sellRngRatio5 := b5.SellRange / totalRawRange5m

		b5.BuyRngEnergy = b5.TotalRngEnergy * buyRngRatio5
		b5.SellRngEnergy = b5.TotalRngEnergy * sellRngRatio5
	}

	if s.wsHub != nil {
		key5m := tick.Raw.StockName + ":5m"
		s.wsHub.BroadcastJSON(key5m, map[string]any{
			"type": "bar",
			"data": b5,
		})
	}

	// --- ALERT LOGIC HERE ---
	energyDelta := b1.BuyVolEnergy - b1.SellVolEnergy
	threshold := 0.8

	// The exitThreshold is lower than the entry threshold (e.g., 0.4).
	// This prevents "flickering" alerts if the energy bounces between 0.79 and 0.81.
	exitThreshold := 0.4

	lastState := s.alertStates[token] // Expected values: "BUY", "SELL", or "" (Neutral)
	newState := lastState

	// 1. Determine the New State
	if energyDelta > threshold {
		newState = "BUY"
	} else if energyDelta < -threshold {
		newState = "SELL"
	} else if math.Abs(energyDelta) < exitThreshold {
		// Only revert to neutral if the energy drops significantly
		newState = "NEUTRAL"
	}

	// 2. Trigger ONLY if the state has changed
	if newState != lastState {
		s.alertStates[token] = newState

		alert := models.PlayableAlert{
			Timestamp:   ts,
			StockName:   name,
			Token:       token,
			LastPrice:   price,
			EnergyDelta: energyDelta,
			TotalEnergy: b1.TotalVolEnergy,
			BuyEnergy:   b1.BuyVolEnergy,
			SellEnergy:  b1.SellVolEnergy,
			Timeframe:   "1m",
			// Note: You may want to add a 'Side' or 'State' string field
			// to your PlayableAlert model to easily filter in the UI.
		}

		// Trigger the callback (useful for sound effects or logs)
		if s.onAlert != nil {
			s.onAlert(alert)
		}

		// Broadcast to the global channel.
		// The UI can use this to add/remove/update rows in the Alert Table.
		if s.wsHub != nil {
			s.wsHub.BroadcastJSON("global:alerts", map[string]any{
				"type":  "alert_state_change",
				"state": newState,
				"data":  alert,
			})
		}
	}

	// -------------------------
	// 7. ASSIGN STATS (For UI/Debugging)
	// -------------------------
	tick.Stats = &models.TradeStats{
		MinuteIndex: minuteIndex,
		Timestamp:   ts,

		Volume1m: r.Volume,
		Range1m:  range1m,
		VolumeZ:  volumeZ,
		RangeZ:   rangeZ,

		// The 6 Fact Columns
		TotalVolEnergy: b1.TotalVolEnergy,
		BuyVolEnergy:   b1.BuyVolEnergy,
		SellVolEnergy:  b1.SellVolEnergy,

		TotalRngEnergy: b1.TotalRngEnergy,
		BuyRngEnergy:   b1.BuyRngEnergy,
		SellRngEnergy:  b1.SellRngEnergy,
	}

	return nil
}

func (s *BarBuilderStage) ClearState() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear the alert tracking map
	s.alertStates = make(map[uint32]string)

	// Optional: Clear bars and rolling states if you want a total reset
	s.bar1m = make(map[uint32]*models.Bar)
	s.bar5m = make(map[uint32]*models.Bar)
	s.rolling = make(map[uint32]*RollingState)
}
