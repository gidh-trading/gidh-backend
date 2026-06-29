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
	PriceRank       int32     `json:"price_rank"`
	POC             float64   `json:"poc"`
	VAH             float64   `json:"vah"`
	VAL             float64   `json:"val"`
	Active          bool      `json:"active"`
}

type alertState struct {
	TriggerType      string
	FirstTriggerTime time.Time
	LastEvalTime     time.Time
}

// alertKey allows us to track multiple distinct alerts for the same token
type alertKey struct {
	Token       uint32
	TriggerType string
}

type ScoutStage struct {
	wsHub        *ws.Hub
	profiles     map[uint32]*models.InstrumentProfile
	mu           sync.Mutex
	activeAlerts map[alertKey]alertState
	alertHistory map[uint32][]ScoutHistoricalSnapshot
}

func NewScoutStage(hub *ws.Hub, profiles map[uint32]*models.InstrumentProfile) *ScoutStage {
	return &ScoutStage{
		wsHub:        hub,
		profiles:     profiles,
		activeAlerts: make(map[alertKey]alertState),
		alertHistory: make(map[uint32][]ScoutHistoricalSnapshot),
	}
}

// ProcessClosedBar evaluates scout alerts at the close of a 1-minute bar
func (s *ScoutStage) ProcessClosedBar(bar *models.Bar) error {
	if s.wsHub == nil || bar.Timeframe != "1m" {
		return nil
	}

	token := uint32(bar.InstrumentToken)

	// Map out the 4 independent trigger conditions
	conditions := map[string]bool{
		"ADR_HIGH_TOUCH":  bar.High >= bar.Analytics.ADRHigh && bar.Analytics.ADRHigh > 0,
		"ADR_LOW_TOUCH":   bar.Low <= bar.Analytics.ADRLow && bar.Analytics.ADRLow > 0,
		"VWAP_ALERT_High": bar.Analytics.NormalizedVwapDistance > 0.5,
		"VWAP_ALERT_Low":  bar.Analytics.NormalizedVwapDistance < -0.5,
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for triggerType, isTriggered := range conditions {
		key := alertKey{
			Token:       token,
			TriggerType: triggerType,
		}

		state, hasActiveAlert := s.activeAlerts[key]

		if isTriggered {
			// If it wasn't already active, fire a new alert
			if !hasActiveAlert {
				s.activeAlerts[key] = alertState{
					TriggerType:      triggerType,
					FirstTriggerTime: bar.Timestamp,
					LastEvalTime:     time.Now(),
				}

				snapshot := s.compileSnapshot(bar, triggerType, true)
				s.alertHistory[token] = append(s.alertHistory[token], snapshot)
				s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})
			}
			// (If already active, we just hold the state and do nothing until it turns off)
		} else {
			// If it was active but the condition is no longer met, turn it off
			if hasActiveAlert {
				snapshot := s.compileSnapshot(bar, state.TriggerType, false)
				s.alertHistory[token] = append(s.alertHistory[token], snapshot)
				s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})
				delete(s.activeAlerts, key)
			}
		}
	}

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
		// Return the most recent state for the token
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

// compileSnapshot constructs the alert payload from a closed bar
func (s *ScoutStage) compileSnapshot(bar *models.Bar, trigger string, isActive bool) ScoutHistoricalSnapshot {
	return ScoutHistoricalSnapshot{
		Timestamp:       bar.Timestamp,
		InstrumentToken: uint32(bar.InstrumentToken),
		StockName:       bar.StockName,
		TriggerType:     trigger,
		Price:           bar.Close,
		VolumeRank:      int32(bar.Analytics.VolumeRank),
		TickRank:        int32(bar.Analytics.TickRank),
		PriceRank:       int32(bar.Analytics.PriceRank),
		POC:             bar.POC,
		VAH:             bar.VAH,
		VAL:             bar.VAL,
		Active:          isActive,
	}
}
