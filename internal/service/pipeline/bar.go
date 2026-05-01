package pipeline

import (
	"gidh-backend/internal/service/models"
	"sync"
	"time"
)

// -------------------------------
// ROLLING ENTRY (for O(1))
// -------------------------------
type rollingEntry struct {
	ts    time.Time
	price float64
	vol   float64
}

type RollingState struct {
	queue []rollingEntry

	Volume float64
	Open   float64
	High   float64
	Low    float64
	Close  float64
}

type Bar struct {
	Open   float64
	High   float64
	Low    float64
	Close  float64
	Volume float64
	Start  time.Time
}

type SessionState struct {
	TotalVolume float64
	TotalRange  float64
	Count       int
}

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
	bar1m   map[uint32]*Bar
	bar5m   map[uint32]*Bar
	session map[uint32]*SessionState

	mu sync.RWMutex
}

func NewBarBuilderStage() *BarBuilderStage {
	loc, _ := time.LoadLocation("Asia/Kolkata")

	return &BarBuilderStage{
		loc:     loc,
		rolling: make(map[uint32]*RollingState),
		bar1m:   make(map[uint32]*Bar),
		bar5m:   make(map[uint32]*Bar),
		session: make(map[uint32]*SessionState),
	}
}

func (s *BarBuilderStage) Process(tick *models.EnrichedTick) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	vol := float64(tick.TickVolume)
	ts := tick.Raw.Timestamp.In(s.loc)

	if tick.DNA == nil {
		return nil
	}

	// init
	if s.rolling[token] == nil {
		s.rolling[token] = &RollingState{}
	}
	if s.bar1m[token] == nil {
		s.bar1m[token] = newBar(ts, price)
	}
	if s.bar5m[token] == nil {
		s.bar5m[token] = newBar(ts, price)
	}
	if s.session[token] == nil {
		s.session[token] = &SessionState{}
	}

	r := s.rolling[token]
	session := s.session[token]

	// -------------------------
	// 1. UPDATE ROLLING (O(1))
	// -------------------------
	entry := rollingEntry{ts, price, vol}
	r.queue = append(r.queue, entry)
	r.Volume += vol

	// init open
	if len(r.queue) == 1 {
		r.Open = price
		r.High = price
		r.Low = price
	}

	// update OHLC
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

	// recompute high/low only when needed (cheap amortized)
	if len(r.queue) > 0 {
		r.Open = r.queue[0].price

		// lazy recompute only if needed
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
	updateBar(b1, price, vol)

	if ts.Minute() != b1.Start.Minute() {
		session.Update(b1.Volume, b1.High-b1.Low)
		s.bar1m[token] = newBar(ts, price)
		b1 = s.bar1m[token]
	}

	// -------------------------
	// 3. UPDATE 5M BAR
	// -------------------------
	b5 := s.bar5m[token]
	updateBar(b5, price, vol)

	if (ts.Minute() / 5) != (b5.Start.Minute() / 5) {
		session.Update(b5.Volume, b5.High-b5.Low)
		s.bar5m[token] = newBar(ts, price)
		b5 = s.bar5m[token]
	}

	// -------------------------
	// 4. COMPUTE STATS
	// -------------------------
	if session.TotalVolume == 0 || session.AvgRange() == 0 {
		return nil
	}

	range1m := r.High - r.Low
	normVol := r.Volume / session.TotalVolume
	normRange := range1m / session.AvgRange()

	// minute index
	marketOpen := time.Date(ts.Year(), ts.Month(), ts.Day(), 9, 15, 0, 0, s.loc)
	minuteIndex := int(ts.Sub(marketOpen).Minutes())

	if minuteIndex < 0 || minuteIndex >= len(tick.DNA.TimeBuckets) {
		return nil
	}

	bucket := tick.DNA.TimeBuckets[minuteIndex]

	volumeZ := (normVol - bucket.VolumeMean) / bucket.VolumeStd
	rangeZ := (normRange - bucket.RangeMean) / bucket.RangeStd

	// -------------------------
	// 5. ASSIGN STATS
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
	}

	return nil
}

// -------------------------------
// HELPERS
// -------------------------------
func newBar(ts time.Time, price float64) *Bar {
	return &Bar{
		Open:  price,
		High:  price,
		Low:   price,
		Close: price,
		Start: ts,
	}
}

func updateBar(b *Bar, price, vol float64) {
	if price > b.High {
		b.High = price
	}
	if price < b.Low {
		b.Low = price
	}
	b.Close = price
	b.Volume += vol
}
