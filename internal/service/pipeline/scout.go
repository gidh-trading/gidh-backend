// internal/service/pipeline/scout.go
package pipeline

import (
	"sort"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/ws"
)

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

type cachedBoundaries struct {
	POC float64
	VAH float64
	VAL float64
}

type alertState struct {
	TriggerType      string
	FirstTriggerTime time.Time
	LastEvalTime     time.Time
}

type ScoutStage struct {
	wsHub        *ws.Hub
	profiles     map[uint32]*models.InstrumentProfile
	mu           sync.Mutex
	activeAlerts map[uint32]alertState
	alertHistory map[uint32][]ScoutHistoricalSnapshot
	profileCache map[uint32]cachedBoundaries
}

func NewScoutStage(hub *ws.Hub, profiles map[uint32]*models.InstrumentProfile) *ScoutStage {
	return &ScoutStage{
		wsHub:        hub,
		profiles:     profiles,
		activeAlerts: make(map[uint32]alertState),
		alertHistory: make(map[uint32][]ScoutHistoricalSnapshot),
		profileCache: make(map[uint32]cachedBoundaries),
	}
}

func (s *ScoutStage) Process(tick *models.EnrichedTick) error {
	if s.wsHub == nil {
		return nil
	}

	token := tick.Raw.InstrumentToken
	price := tick.Raw.LastPrice
	volRank := tick.Enrichment.VolumeRank
	tickRank := tick.Enrichment.TickRank
	priceRank := tick.Enrichment.PriceRank // Extraction of normalized price velocity

	// 🟢 RULE 1: FILTER PACKET CHATTER NOISE
	if volRank == 0 && tickRank == 0 {
		return nil
	}

	s.mu.Lock()
	state, hasActiveAlert := s.activeAlerts[token]
	prof, hasProfile := s.profiles[token]

	if tick.VolProfile != nil && tick.VolProfile.VAH > 0 && tick.VolProfile.VAL > 0 {
		s.profileCache[token] = cachedBoundaries{
			POC: tick.VolProfile.POC,
			VAH: tick.VolProfile.VAH,
			VAL: tick.VolProfile.VAL,
		}
	}
	cached, hasCached := s.profileCache[token]
	s.mu.Unlock()

	// ==========================================================
	// ⚡ THE SPARK GATEKEEPER CONDITION
	// ==========================================================
	// Only validate and process triggers if price movement is actively crossing
	// or exceeding the P50 median threshold context.
	hasSubstantialPriceMove := priceRank >= 4

	now := time.Now()
	var currentTrigger string

	var dynamicBuffer float64 = 0.0
	if hasProfile && prof != nil && prof.ATR14 > 0 {
		dynamicBuffer = 0.05 * float64(prof.ATR14)
	}

	// ==========================================================
	// 0. PROFILE MATURITY CHECK (9:30 AM GRACE PERIOD)
	// ==========================================================
	loc, _ := time.LoadLocation("Asia/Kolkata")
	exchangeTime := tick.Raw.Timestamp.In(loc)

	isProfileMature := true
	if (exchangeTime.Hour() == 9 && exchangeTime.Minute() < 30) || exchangeTime.Hour() < 9 {
		isProfileMature = false
	}

	// ==========================================================
	// 1. STATE RE-EVALUATION LATCH
	// ==========================================================
	if hasActiveAlert {
		if state.TriggerType == "VOLUME_SPIKE" {
			if (volRank >= 6 || tickRank >= 6) && hasSubstantialPriceMove {
				currentTrigger = "VOLUME_SPIKE"
			}
		} else if state.TriggerType == "VAH_BREACH" {
			if hasCached && price >= cached.VAH && hasSubstantialPriceMove {
				currentTrigger = "VAH_BREACH"
			}
		} else if state.TriggerType == "VAL_BREACH" {
			if hasCached && price <= cached.VAL && hasSubstantialPriceMove {
				currentTrigger = "VAL_BREACH"
			}
		}
	}

	// ==========================================================
	// 2. FRESH ANOMALY SENSING (Only processed if currently idle)
	// ==========================================================
	if currentTrigger == "" && !hasActiveAlert && hasSubstantialPriceMove {
		if volRank >= 6 || tickRank >= 6 { // Syncs to P97 linear scale boundary
			currentTrigger = "VOLUME_SPIKE"
		} else if isProfileMature && hasCached {
			if price > (cached.VAH + dynamicBuffer) {
				currentTrigger = "VAH_BREACH"
			} else if price < (cached.VAL - dynamicBuffer) {
				currentTrigger = "VAL_BREACH"
			}
		}
	}

	// ==========================================================
	// 3. BROADCAST LAYER & HANDSHAKE MANAGEMENT
	// ==========================================================
	s.mu.Lock()
	defer s.mu.Unlock()

	// Leg A: Anomaly ended. Send single active: false payload to turn off UI highlight
	if currentTrigger == "" && hasActiveAlert {
		snapshot := s.compileSnapshot(tick, cached, state.FirstTriggerTime, state.TriggerType, false)
		s.alertHistory[token] = append(s.alertHistory[token], snapshot)
		s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})

		delete(s.activeAlerts, token)
		return nil
	}

	// Leg B: Stable equilibrium. Exit quietly.
	if currentTrigger == "" {
		return nil
	}

	// Leg C: Ongoing sustained breakout session. Send updates every 1 minute.
	if hasActiveAlert && state.TriggerType == currentTrigger {
		if now.Sub(state.LastEvalTime) < 1*time.Minute {
			return nil
		}

		state.LastEvalTime = now
		s.activeAlerts[token] = state

		snapshot := s.compileSnapshot(tick, cached, state.FirstTriggerTime, currentTrigger, true)
		s.alertHistory[token] = append(s.alertHistory[token], snapshot)
		s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})
		return nil
	}

	// Leg D: Fresh directional expansion crossover event initialized.
	s.activeAlerts[token] = alertState{
		TriggerType:      currentTrigger,
		FirstTriggerTime: tick.Raw.Timestamp,
		LastEvalTime:     now,
	}

	snapshot := s.compileSnapshot(tick, cached, tick.Raw.Timestamp, currentTrigger, true)
	s.alertHistory[token] = append(s.alertHistory[token], snapshot)
	s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})
	return nil
}

func (s *ScoutStage) GetAllAlertHistory() []ScoutHistoricalSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()

	var dynamicMatrix []ScoutHistoricalSnapshot

	for _, snapshots := range s.alertHistory {
		if len(snapshots) == 0 {
			continue
		}
		dynamicMatrix = append(dynamicMatrix, snapshots[len(snapshots)-1])
	}

	sort.Slice(dynamicMatrix, func(i, j int) bool {
		if dynamicMatrix[i].Active && !dynamicMatrix[j].Active {
			return true
		}
		if !dynamicMatrix[i].Active && dynamicMatrix[j].Active {
			return false
		}
		return dynamicMatrix[i].Timestamp.After(dynamicMatrix[j].Timestamp)
	})

	return dynamicMatrix
}

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

func (s *ScoutStage) compileSnapshot(tick *models.EnrichedTick, cached cachedBoundaries, triggerTime time.Time, trigger string, isActive bool) ScoutHistoricalSnapshot {
	var outPoc, outVah, outVal float64
	if tick.VolProfile != nil && tick.VolProfile.VAH > 0 {
		outPoc = tick.VolProfile.POC
		outVah = tick.VolProfile.VAH
		outVal = tick.VolProfile.VAL
	} else {
		outPoc = cached.POC
		outVah = cached.VAH
		outVal = cached.VAL
	}

	return ScoutHistoricalSnapshot{
		Timestamp:       triggerTime,
		InstrumentToken: tick.Raw.InstrumentToken,
		StockName:       tick.Raw.StockName,
		TriggerType:     trigger,
		Price:           tick.Raw.LastPrice,
		VolumeRank:      int32(tick.Enrichment.VolumeRank),
		TickRank:        int32(tick.Enrichment.TickRank),
		POC:             outPoc,
		VAH:             outVah,
		VAL:             outVal,
		Active:          isActive,
	}
}
