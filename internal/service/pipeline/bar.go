package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
	"gidh-backend/pkg/logger"
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
	loc       *time.Location
	rolling   map[uint32]*RollingState
	bar1m     map[uint32]*models.Bar
	bar5m     map[uint32]*models.Bar
	session   map[uint32]*SessionState
	adv30dMap map[uint32]float64
	lastTs    map[uint32]time.Time
	mu        sync.RWMutex
	writer    *writer.DBWriter
}

func NewBarBuilderStage(w *writer.DBWriter, advMap map[uint32]float64) *BarBuilderStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &BarBuilderStage{
		loc:       loc,
		rolling:   make(map[uint32]*RollingState),
		bar1m:     make(map[uint32]*models.Bar),
		bar5m:     make(map[uint32]*models.Bar),
		session:   make(map[uint32]*SessionState),
		adv30dMap: advMap,
		lastTs:    make(map[uint32]time.Time),
		writer:    w,
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
		b1.Ticks = append(b1.Ticks, tick.Raw)
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
	}

	// -------------------------
	// 4. TICK DIRECTION & RAW ACCUMULATION
	// -------------------------
	dir := 0
	if price > prevPrice {
		dir = 1
	} else if price < prevPrice {
		dir = -1
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

	logger.Infof("adv30d: %v", adv30d)

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
