package pipeline

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"

	"github.com/jackc/pgx/v5/pgxpool"
)

type VolumeProfileStage struct {
	profiles map[uint32]*models.VolumeProfile
	mu       sync.RWMutex
	pool     *pgxpool.Pool
}

func NewVolumeProfileStage(configs []models.InstrumentConfig, pool *pgxpool.Pool) *VolumeProfileStage {
	h := &VolumeProfileStage{
		profiles: make(map[uint32]*models.VolumeProfile),
		pool:     pool,
	}

	for _, cfg := range configs {
		h.profiles[cfg.Token] = &models.VolumeProfile{
			StockName:       cfg.Name,
			InstrumentToken: cfg.Token,
			BucketSize:      1.0, // Default to 1.0, adjust via config if needed
			Buckets:         make(map[float64]int64),
			SortedPrices:    make([]float64, 0),
		}
	}
	return h
}

// Process orchestrates the Auction Market Theory logic for a single tick.
func (h *VolumeProfileStage) Process(tick *models.EnrichedTick) error {
	h.mu.RLock()
	p, exists := h.profiles[tick.Raw.InstrumentToken]
	h.mu.RUnlock()

	if !exists || tick.TickVolume == 0 {
		return nil
	}

	p.Mu.Lock()
	defer p.Mu.Unlock()

	// 1. Initialize session date if fresh
	if p.TradingDate.IsZero() {
		p.TradingDate = tick.Raw.Timestamp.Truncate(24 * time.Hour)
	}

	// 2. Assign price to discrete bucket
	bucketPrice := math.Floor(tick.Raw.LastPrice/p.BucketSize) * p.BucketSize

	if _, ok := p.Buckets[bucketPrice]; !ok {
		idx := sort.SearchFloat64s(p.SortedPrices, bucketPrice)
		p.SortedPrices = append(p.SortedPrices, 0)
		copy(p.SortedPrices[idx+1:], p.SortedPrices[idx:])
		p.SortedPrices[idx] = bucketPrice
	}

	p.Buckets[bucketPrice] += tick.TickVolume
	p.TotalVolume += tick.TickVolume
	p.TickCount++

	// 3. Calculate POC (Point of Control)
	maxVol := int64(-1)
	for price, vol := range p.Buckets {
		if vol > maxVol {
			maxVol = vol
			p.POC = price
		}
	}

	// 4. Calculate Value Area (VAH/VAL) covering 70% of volume
	if p.TotalVolume > 0 && p.POC > 0 {
		targetVolume := float64(p.TotalVolume) * 0.70
		currentVolume := float64(p.Buckets[p.POC])

		pocIdx := sort.SearchFloat64s(p.SortedPrices, p.POC)
		lowIdx, highIdx := pocIdx, pocIdx

		for currentVolume < targetVolume {
			volUp, volDown := 0.0, 0.0
			if highIdx+1 < len(p.SortedPrices) {
				volUp = float64(p.Buckets[p.SortedPrices[highIdx+1]])
			}
			if lowIdx-1 >= 0 {
				volDown = float64(p.Buckets[p.SortedPrices[lowIdx-1]])
			}
			if volUp == 0 && volDown == 0 {
				break
			}

			if volUp >= volDown {
				currentVolume += volUp
				highIdx++
			} else {
				currentVolume += volDown
				lowIdx--
			}
		}
		p.VAH = p.SortedPrices[highIdx]
		p.VAL = p.SortedPrices[lowIdx]
	}

	// 5. Context Enrichment for downstream stages
	tick.VolProfile = &models.VolumeProfileInfo{
		POC: p.POC,
		VAH: p.VAH,
		VAL: p.VAL,
	}

	// 6. Persistence: Archive to DB strictly every 20 ticks
	if p.TickCount%20 == 0 {
		h.syncAllBucketsToNodes(p)
		snapshot := p.Copy()
		go h.persistSingleProfileAsync(snapshot)
	}

	return nil
}

func (h *VolumeProfileStage) syncAllBucketsToNodes(p *models.VolumeProfile) {
	n := len(p.SortedPrices)
	if n < 3 {
		return
	}

	nodes := make([]models.VPNode, n)
	var pocVolume int64

	for i, price := range p.SortedPrices {
		vol := p.Buckets[price]
		nodes[i] = models.VPNode{Price: price, Volume: vol}
		if price == p.POC {
			pocVolume = vol
		}
	}
	if pocVolume == 0 {
		pocVolume = 1
	}
	p.Nodes = nodes

	// Light smoothing
	smoothed := make([]float64, n)
	for i := 0; i < n; i++ {
		sum := float64(nodes[i].Volume)
		count := 1.0
		if i > 0 {
			sum += float64(nodes[i-1].Volume)
			count++
		}
		if i < n-1 {
			sum += float64(nodes[i+1].Volume)
			count++
		}
		smoothed[i] = sum / count
	}

	var hvns, lvns []models.VPExtrema

	for i := 1; i < n-1; i++ {
		curr, prev, next := smoothed[i], smoothed[i-1], smoothed[i+1]
		volPct := (float64(nodes[i].Volume) / float64(pocVolume)) * 100.0

		// HVN (Peak)
		if curr >= prev && curr >= next && (curr > prev || curr > next) && volPct >= 5 {
			hvns = append(hvns, models.VPExtrema{Price: nodes[i].Price, Volume: nodes[i].Volume, Strength: volPct})
		}
		// LVN (Valley)
		if curr <= prev && curr <= next && (curr < prev || curr < next) {
			avgNeighbors := (prev + next) / 2
			if curr == 0 {
				curr = 1
			}
			if strength := avgNeighbors / curr; strength > 1.2 {
				lvns = append(lvns, models.VPExtrema{Price: nodes[i].Price, Volume: nodes[i].Volume, Strength: strength * 10})
			}
		}
	}

	if hvns == nil {
		hvns = []models.VPExtrema{}
	}
	if lvns == nil {
		lvns = []models.VPExtrema{}
	}
	p.HVNs = hvns
	p.LVNs = lvns
}

func (h *VolumeProfileStage) persistSingleProfileAsync(p *models.VolumeProfile) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	nodesJSON, _ := json.Marshal(p.Nodes)
	hvnsJSON, _ := json.Marshal(p.HVNs)
	lvnsJSON, _ := json.Marshal(p.LVNs)

	_, err := h.pool.Exec(ctx, `
		INSERT INTO gidh_volume_profiles (
			instrument_token, stock_name, trading_date, total_volume, 
			poc, vah, val, nodes, hvns, lvns, updated_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
		ON CONFLICT (instrument_token, trading_date) DO UPDATE SET
			total_volume = EXCLUDED.total_volume,
			poc = EXCLUDED.poc,
			vah = EXCLUDED.vah,
			val = EXCLUDED.val,
			nodes = EXCLUDED.nodes,
			hvns = EXCLUDED.hvns,
			lvns = EXCLUDED.lvns,
			updated_at = NOW()`,
		p.InstrumentToken, p.StockName, p.TradingDate, p.TotalVolume,
		p.POC, p.VAH, p.VAL, nodesJSON, hvnsJSON, lvnsJSON,
	)

	if err != nil {
		logger.Errorf("VP Persistence Error for %s: %v", p.StockName, err)
	}
}

// LoadExistingProfiles reconstructs the RAM state from the DB if the backend restarts mid-session.
func (h *VolumeProfileStage) LoadExistingProfiles(ctx context.Context, targetDate time.Time) error {
	dateStr := targetDate.Format("2006-01-02")
	logger.Infof("Reconstructing Volume Profiles for date: %s", dateStr)

	rows, err := h.pool.Query(ctx, `
		SELECT instrument_token, trading_date, total_volume, poc, vah, val, nodes, hvns, lvns
		FROM gidh_volume_profiles
		WHERE trading_date = $1`, dateStr)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var token uint32
		var tDate time.Time
		var totalVol int64
		var poc, vah, val float64
		var nodesJSON, hvnsJSON, lvnsJSON []byte

		if err := rows.Scan(&token, &tDate, &totalVol, &poc, &vah, &val, &nodesJSON, &hvnsJSON, &lvnsJSON); err != nil {
			continue
		}

		h.mu.RLock()
		p, ok := h.profiles[token]
		h.mu.RUnlock()
		if !ok {
			continue
		}

		p.Mu.Lock()
		p.TradingDate = tDate
		p.TotalVolume = totalVol
		p.POC = poc
		p.VAH = vah
		p.VAL = val

		if len(nodesJSON) > 0 {
			var nodes []models.VPNode
			if json.Unmarshal(nodesJSON, &nodes) == nil {
				p.Nodes = nodes
				p.Buckets = make(map[float64]int64)
				p.SortedPrices = make([]float64, 0, len(nodes))
				for _, n := range nodes {
					p.Buckets[n.Price] = n.Volume
					p.SortedPrices = append(p.SortedPrices, n.Price)
				}
				sort.Float64s(p.SortedPrices)
			}
		}
		if len(hvnsJSON) > 0 {
			json.Unmarshal(hvnsJSON, &p.HVNs)
		}
		if len(lvnsJSON) > 0 {
			json.Unmarshal(lvnsJSON, &p.LVNs)
		}
		p.Mu.Unlock()
	}
	return nil
}
