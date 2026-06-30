// internal/service/pipeline/scout.go
package pipeline

import (
	"sort"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/ws"
	"gidh-backend/pkg/logger" // Imported the backend logging library
)

type ScoutHistoricalSnapshot struct {
	Timestamp        time.Time `json:"timestamp"`
	InstrumentToken  uint32    `json:"instrument_token"`
	StockName        string    `json:"stock_name"`
	TriggerType      string    `json:"trigger_type"`
	Price            float64   `json:"price"`
	ChangePct        float64   `json:"change_pct"`
	ADRHigh          float64   `json:"adr_high"`
	ADRLow           float64   `json:"adr_low"`
	VwapDistance     float64   `json:"vwap_distance"`
	VolumeRank       int32     `json:"volume_rank"`
	PriceRank        int32     `json:"price_rank"`
	Nifty50ChangePct float64   `json:"nifty50_change_pct"`
	Active           bool      `json:"active"`
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
	alertHistory map[alertKey][]ScoutHistoricalSnapshot
}

func NewScoutStage(hub *ws.Hub, profiles map[uint32]*models.InstrumentProfile) *ScoutStage {
	logger.Info("[Scout Stage] Initializing Scout Execution Stage Matrix...")
	return &ScoutStage{
		wsHub:        hub,
		profiles:     profiles,
		activeAlerts: make(map[alertKey]alertState),
		alertHistory: make(map[alertKey][]ScoutHistoricalSnapshot),
	}
}

// ProcessClosedBar evaluates scout alerts at the close of a 1-minute bar
func (s *ScoutStage) ProcessClosedBar(bar *models.Bar) error {
	if s.wsHub == nil {
		logger.Warn("[Scout Stage] Evaluation skipped: WebScoket Hub reference is nil.")
		return nil
	}
	if bar.Timeframe != "1m" {
		return nil
	}

	token := uint32(bar.InstrumentToken)

	// Map out the 4 independent trigger conditions
	conditions := map[string]bool{
		"ADR_HIGH_TOUCH":      bar.High >= bar.Analytics.ADRHigh && bar.Analytics.ADRHigh > 0,
		"ADR_LOW_TOUCH":       bar.Low <= bar.Analytics.ADRLow && bar.Analytics.ADRLow > 0,
		"VWAP_ALERT_High":     bar.Analytics.NormalizedVwapDistance > 0.5,
		"VWAP_ALERT_Low":      bar.Analytics.NormalizedVwapDistance < -0.5,
		"FLOW_INTENSITY_HIGH": bar.Analytics.RollingFlowIntensity >= 6,
	}

	// Trace print to log terminal metrics per evaluation cycle
	logger.Debugf("[Scout Evaluate] Token: %d (%s) | High: %.2f | Low: %.2f | ADR_H: %.2f | ADR_L: %.2f | VWAP_Dist: %.4f",
		token, bar.StockName, bar.High, bar.Low, bar.Analytics.ADRHigh, bar.Analytics.ADRLow, bar.Analytics.NormalizedVwapDistance)

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
				logger.Debugf("🚨 [Scout ALERT TRIGGER] Stock: %s | Type: %s | Price: %.2f | Active: TRUE", bar.StockName, triggerType, bar.Close)

				s.activeAlerts[key] = alertState{
					TriggerType:      triggerType,
					FirstTriggerTime: bar.Timestamp,
					LastEvalTime:     time.Now(),
				}

				snapshot := s.compileSnapshot(bar, triggerType, true)
				s.alertHistory[key] = append(s.alertHistory[key], snapshot)

				// Broadcast tracking sequence
				logger.Debugf("[Scout Broadcast] Dispatching to WebSockets payload type: scout_alert for %s", bar.StockName)
				s.wsHub.BroadcastJSON("global:trading", map[string]any{"type": "scout_alert", "data": snapshot})
			}
		} else {
			// If it was active but the condition is no longer met, turn it off
			if hasActiveAlert {
				logger.Debugf("✅ [Scout ALERT CONCLUDED] Stock: %s | Type: %s | Price: %.2f | Active: FALSE", bar.StockName, state.TriggerType, bar.Close)

				snapshot := s.compileSnapshot(bar, state.TriggerType, false)
				s.alertHistory[key] = append(s.alertHistory[key], snapshot)

				logger.Debugf("[Scout Broadcast] Dispatching conclusion to WebSockets payload type: scout_alert for %s", bar.StockName)
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
		dynamicMatrix = append(dynamicMatrix, snapshots[len(snapshots)-1])
	}

	logger.Debugf("[Scout API] GetAllAlertHistory requested. Aggregated unique rows processed: %d", len(dynamicMatrix))

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

	var dst []ScoutHistoricalSnapshot

	for key, snapshots := range s.alertHistory {
		if key.Token == token {
			dst = append(dst, snapshots...)
		}
	}

	sort.Slice(dst, func(i, j int) bool {
		return dst[i].Timestamp.Before(dst[j].Timestamp)
	})

	return dst
}

// compileSnapshot constructs the alert payload from a closed bar
func (s *ScoutStage) compileSnapshot(bar *models.Bar, trigger string, isActive bool) ScoutHistoricalSnapshot {
	return ScoutHistoricalSnapshot{
		Timestamp:        bar.Timestamp,
		InstrumentToken:  uint32(bar.InstrumentToken),
		StockName:        bar.StockName,
		TriggerType:      trigger,
		Price:            bar.Close,
		VolumeRank:       int32(bar.Analytics.VolumeRank),
		PriceRank:        int32(bar.Analytics.PriceRank),
		ChangePct:        bar.ChangePct,
		Nifty50ChangePct: bar.Analytics.Nifty50ChangePct,
		ADRHigh:          bar.Analytics.ADRHigh,
		ADRLow:           bar.Analytics.ADRLow,
		VwapDistance:     bar.Analytics.NormalizedVwapDistance,
		Active:           isActive,
	}
}
