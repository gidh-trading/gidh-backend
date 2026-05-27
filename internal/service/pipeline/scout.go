// internal/service/pipeline/scout.go
package pipeline

import (
	"sort"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/ws"
)

// ScoutHistoricalSnapshot matches the exact JSON schema your UI layout expects
type ScoutHistoricalSnapshot struct {
	Timestamp       time.Time `json:"timestamp"`
	InstrumentToken uint32    `json:"instrument_token"`
	StockName       string    `json:"stock_name"`
	TriggerType     string    `json:"trigger_type"`
	Price           float64   `json:"price"`
	VolumeRank      int32     `json:"volume_rank"`
	TickRank        int32     `json:"tick_rank"`
	POC             float64   `json:"poc"`
	VAH             float64   `json:"vah"`
	VAL             float64   `json:"val"`
	Active          bool      `json:"active"`
}

type ScoutStage struct {
	wsHub          *ws.Hub
	profiles       map[uint32]*models.InstrumentProfile
	mu             sync.Mutex
	lastTrigger    map[uint32]string
	lastAlertTime  map[uint32]time.Time
	lastSeenActive map[uint32]time.Time
	alertHistory   map[uint32][]ScoutHistoricalSnapshot // Map key is InstrumentToken
}

func NewScoutStage(hub *ws.Hub, profiles map[uint32]*models.InstrumentProfile) *ScoutStage {
	return &ScoutStage{
		wsHub:          hub,
		profiles:       profiles,
		lastTrigger:    make(map[uint32]string),
		lastAlertTime:  make(map[uint32]time.Time),
		lastSeenActive: make(map[uint32]time.Time),
		alertHistory:   make(map[uint32][]ScoutHistoricalSnapshot),
	}
}

// GetAlertHistory returns history for a single ticker (used for single stock chart plotting)
func (s *ScoutStage) GetAlertHistory(token uint32) []ScoutHistoricalSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	history, exists := s.alertHistory[token]
	if !exists {
		return []ScoutHistoricalSnapshot{}
	}

	dst := make([]ScoutHistoricalSnapshot, len(history))
	copy(dst, history)
	return dst
}

// GetAllAlertHistory merges and flattens all logs chronologically (used for whole Watchtower table init)
func (s *ScoutStage) GetAllAlertHistory() []ScoutHistoricalSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	var flatHistory []ScoutHistoricalSnapshot
	for _, snapshots := range s.alertHistory {
		flatHistory = append(flatHistory, snapshots...)
	}

	// Sort chronologically by timestamp so the UI receives an orderly timeline sequence
	sort.Slice(flatHistory, func(i, j int) bool {
		return flatHistory[i].Timestamp.Before(flatHistory[j].Timestamp)
	})

	return flatHistory
}

func (s *ScoutStage) Process(tick *models.EnrichedTick) error {
	if s.wsHub == nil {
		return nil
	}

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	volRank := tick.Enrichment.VolumeRank
	tickRank := tick.Enrichment.TickRank

	var currentTrigger string
	active := false

	if volRank >= 6 || tickRank >= 6 {
		currentTrigger = "VOLUME_SPIKE"
		active = true
	}

	if tick.VolProfile != nil && tick.VolProfile.VAH > 0 && tick.VolProfile.VAL > 0 {
		s.mu.Lock()
		prof, hasProfile := s.profiles[token]
		s.mu.Unlock()

		var dynamicBuffer float64 = 0.0
		if hasProfile && prof != nil && prof.ATR14 > 0 {
			dynamicBuffer = 0.05 * prof.ATR14
		}

		if price > (tick.VolProfile.VAH + dynamicBuffer) {
			currentTrigger = "VAH_BREACH"
			active = true
		} else if price < (tick.VolProfile.VAL - dynamicBuffer) {
			currentTrigger = "VAL_BREACH"
			active = true
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	lastTrig := s.lastTrigger[token]
	now := time.Now()

	if active {
		s.lastSeenActive[token] = now
	} else if lastTrig != "" {
		lastActiveTime := s.lastSeenActive[token]
		if now.Sub(lastActiveTime) < 3*time.Second {
			currentTrigger = lastTrig
			active = true
		}
	}

	if currentTrigger == "" {
		if lastTrig != "" {
			s.broadcastAndRecord(tick, lastTrig, false)
			delete(s.lastTrigger, token)
			delete(s.lastAlertTime, token)
			delete(s.lastSeenActive, token)
		}
		return nil
	}

	lastTime := s.lastAlertTime[token]
	if lastTrig == currentTrigger && now.Sub(lastTime) < 1*time.Second {
		return nil
	}

	s.lastTrigger[token] = currentTrigger
	s.lastAlertTime[token] = now

	s.broadcastAndRecord(tick, currentTrigger, active)
	return nil
}

func (s *ScoutStage) broadcastAndRecord(tick *models.EnrichedTick, trigger string, isActive bool) {
	var poc, vah, val float64
	if tick.VolProfile != nil {
		poc = tick.VolProfile.POC
		vah = tick.VolProfile.VAH
		val = tick.VolProfile.VAL
	}

	snapshot := ScoutHistoricalSnapshot{
		Timestamp:       tick.Raw.Timestamp,
		InstrumentToken: tick.Raw.InstrumentToken,
		StockName:       tick.Raw.StockName,
		TriggerType:     trigger,
		Price:           tick.Raw.LastPrice,
		VolumeRank:      int32(tick.Enrichment.VolumeRank),
		TickRank:        int32(tick.Enrichment.TickRank),
		POC:             poc,
		VAH:             vah,
		VAL:             val,
		Active:          isActive,
	}
	s.alertHistory[tick.Raw.InstrumentToken] = append(s.alertHistory[tick.Raw.InstrumentToken], snapshot)

	payload := map[string]any{
		"type": "scout_alert", // 👈 Matches your frontend switch statement case
		"data": map[string]any{
			"timestamp":        tick.Raw.Timestamp,
			"instrument_token": tick.Raw.InstrumentToken,
			"stock_name":       tick.Raw.StockName,
			"trigger_type":     trigger,
			"price":            tick.Raw.LastPrice,
			"volume_rank":      tick.Enrichment.VolumeRank,
			"tick_rank":        tick.Enrichment.TickRank,
			"poc":              poc,
			"vah":              vah,
			"val":              val,
			"active":           isActive,
		},
	}

	// 🔥 FIXED: Changed from "global:alerts" to "global:trading" to route to the open socket
	s.wsHub.BroadcastJSON("global:trading", payload)
}
