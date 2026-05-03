package pipeline

import (
	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/writer"
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
	loc *time.Location

	rolling map[uint32]*RollingState
	bar1m   map[uint32]*models.Bar
	bar5m   map[uint32]*models.Bar
	session map[uint32]*SessionState

	lastTs map[uint32]time.Time

	mu     sync.RWMutex
	writer *writer.DBWriter
}

func NewBarBuilderStage(w *writer.DBWriter) *BarBuilderStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &BarBuilderStage{
		loc:     loc,
		rolling: make(map[uint32]*RollingState),
		bar1m:   make(map[uint32]*models.Bar),
		bar5m:   make(map[uint32]*models.Bar),
		session: make(map[uint32]*SessionState),
		lastTs:  make(map[uint32]time.Time),
		writer:  w,
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

	// Capture range BEFORE update to avoid look-ahead bias
	prevPrice := r.Close
	if prevPrice == 0 {
		prevPrice = price
	}

	prevHigh := r.High
	if prevHigh == 0 {
		prevHigh = price
	}

	prevLow := r.Low
	if prevLow == 0 {
		prevLow = price
	}

	prevRange := prevHigh - prevLow

	// -------------------------
	// 1. ROLLING WINDOW UPDATE
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

	// remove old entries
	cutoff := ts.Add(-60 * time.Second)
	for len(r.queue) > 0 && r.queue[0].ts.Before(cutoff) {
		old := r.queue[0]
		r.queue = r.queue[1:]
		r.Volume -= old.vol
	}

	// recompute high/low (acceptable for v1)
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
	// 2. UPDATE 1M BAR & TICK BUFFER
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
	// 4. NORMALIZATION
	// -------------------------
	if session.TotalVolume == 0 || session.AvgRange() == 0 {
		return nil
	}

	range1m := r.High - r.Low
	normVol := r.Volume / session.TotalVolume
	normRange := range1m / session.AvgRange()

	// -------------------------
	// DIRECTION (tick-level)
	// -------------------------
	delta := price - prevPrice
	dir := 0.0

	// ✅ FIX 1 & 2: Unified base for both scaling and filtering
	base := math.Max(prevRange, 0.001*price)

	// ✅ FIX 3: Volume-weighted direction to respect lot-size mechanics
	volWeight := math.Sqrt(vol) // Dampens massive outliers while respecting volume gravity

	if math.Abs(delta) >= 0.01*base {
		dir = (delta / base) * volWeight
	}

	// clamp
	if dir > 1 {
		dir = 1
	} else if dir < -1 {
		dir = -1
	}

	// -------------------------
	// 5. DNA LOOKUP
	// -------------------------
	marketOpen := time.Date(ts.Year(), ts.Month(), ts.Day(), 9, 15, 0, 0, s.loc)
	minuteIndex := int(ts.Sub(marketOpen).Minutes())

	if minuteIndex < 0 || minuteIndex >= len(tick.DNA.TimeBuckets) {
		return nil
	}

	bucket := tick.DNA.TimeBuckets[minuteIndex]

	// -------------------------
	// 6. Z-SCORE
	// -------------------------
	var volumeZ float64
	if bucket.VolumeStd > 1e-6 {
		volumeZ = (normVol - bucket.VolumeMean) / bucket.VolumeStd
	}

	var rangeZ float64
	if bucket.RangeStd > 1e-6 {
		rangeZ = (normRange - bucket.RangeMean) / bucket.RangeStd
	}

	// -------------------------
	// 7. TIME DELTA (dt)
	// -------------------------
	var dt = 1.0
	if last, ok := s.lastTs[token]; ok {
		dt = ts.Sub(last).Seconds()
		if dt <= 0 || dt > 2 {
			dt = 1.0
		}
	}
	s.lastTs[token] = ts

	// -------------------------
	// 8. ENERGY ACCUMULATION
	// -------------------------
	threshold := 1.5

	vEnergy := math.Max(0, volumeZ-threshold)
	rEnergy := math.Max(0, rangeZ-threshold)

	// ✅ FIX 4: Safe signal sharpening. Prevents NaN when dir is negative.
	sharpDir := math.Pow(math.Abs(dir), 1.5) * math.Copysign(1.0, dir)

	// -------------------------
	// SIGNED ENERGY
	// -------------------------
	signedVol := vEnergy * sharpDir * dt
	signedRng := rEnergy * sharpDir * dt

	// net
	smooth := 0.9

	b1.VolEnergy = smooth*b1.VolEnergy + (1-smooth)*signedVol
	b1.RngEnergy = smooth*b1.RngEnergy + (1-smooth)*signedRng

	b5.VolEnergy = smooth*b5.VolEnergy + (1-smooth)*signedVol
	b5.RngEnergy = smooth*b5.RngEnergy + (1-smooth)*signedRng

	// -------------------------
	// BUY / SELL SPLIT
	// -------------------------
	if sharpDir > 0 {
		b1.BuyVolEnergy += vEnergy * sharpDir * dt
		b1.BuyRngEnergy += rEnergy * sharpDir * dt

		b5.BuyVolEnergy += vEnergy * sharpDir * dt
		b5.BuyRngEnergy += rEnergy * sharpDir * dt

	} else if sharpDir < 0 {
		d := -sharpDir

		b1.SellVolEnergy += vEnergy * d * dt
		b1.SellRngEnergy += rEnergy * d * dt

		b5.SellVolEnergy += vEnergy * d * dt
		b5.SellRngEnergy += rEnergy * d * dt
	}

	// -------------------------
	// ✅ FIX 5 & 6: DERIVED IMBALANCE SIGNAL
	// -------------------------
	imbalance := b1.BuyVolEnergy - b1.SellVolEnergy
	totalEnergy := b1.BuyVolEnergy + b1.SellVolEnergy

	imbalanceRatio := 0.0
	if totalEnergy > 1e-6 {
		imbalanceRatio = imbalance / totalEnergy
	}

	// -------------------------
	// 9. ASSIGN STATS
	// -------------------------
	tick.Stats = &models.TradeStats{
		MinuteIndex: minuteIndex,
		Timestamp:   ts,

		Volume1m: r.Volume,
		Range1m:  range1m,

		SessionVolume:   session.TotalVolume,
		SessionAvgRange: session.AvgRange(),

		NormVolume: normVol,
		NormRange:  normRange,

		VolumeMean: bucket.VolumeMean,
		VolumeStd:  bucket.VolumeStd,
		RangeMean:  bucket.RangeMean,
		RangeStd:   bucket.RangeStd,

		VolumeZ: volumeZ,
		RangeZ:  rangeZ,

		VolEnergy: b1.VolEnergy,
		RngEnergy: b1.RngEnergy,

		BuyVolEnergy:  b1.BuyVolEnergy,
		SellVolEnergy: b1.SellVolEnergy,
		BuyRngEnergy:  b1.BuyRngEnergy,
		SellRngEnergy: b1.SellRngEnergy,

		// The new alpha signal
		EnergyImbalance: imbalanceRatio,
	}

	return nil
}
