package agent

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/pkg/logger"
)

type RiskManager struct {
	mu             sync.RWMutex
	orderManager   order.PositionManager
	scalper        *ScalperAgent
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
	lastExitTime   map[string]time.Time

	// ⚡ Money Management Team additions: Track explicit fee overheads
	dailyChargesPaid float64
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:     om,
		scalper:          sa,
		agentPositions:   make(map[string]*models.Position),
		lastExitTime:     make(map[string]time.Time),
		dailyRealized:    0.0,
		dailyChargesPaid: 0.0, // Start day with clean metrics ledger
		circuitBroken:    false,
	}
}

func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	rm.mu.Lock()
	if rm.circuitBroken {
		rm.mu.Unlock()
		return
	}

	rawTick := enrichedTick.Raw
	symbol := rawTick.StockName
	key := fmt.Sprintf("%s:MIS", symbol)

	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// 1. Money Protection: Check current running exposure against P&L AND accumulated tax drag
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier

		// ⚡ The Real Equation: True Net Session Return = Trading P&L - Total Statutory Fees paid so far
		totalNetSessionPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		if totalNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Money Manager] True Capital Drawdown Breached (₹%.2f). Freezing Agent.", totalNetSessionPnL)
			rm.circuitBroken = true
			rm.executeBrokerOrder(symbol, pos, "Net Session Risk Floor Breach", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}

		// Handle normal intraday square-off times
		loc, _ := time.LoadLocation("Asia/Kolkata")
		if rawTick.Timestamp.In(loc).Hour() == 15 && rawTick.Timestamp.In(loc).Minute() >= 15 {
			rm.executeBrokerOrder(symbol, pos, "Intraday 15:15 Force Square-off", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}
	}

	currentSide := pos.Side
	rm.mu.Unlock()

	// 2. Delegate to Analytics
	decision, triggered := rm.scalper.AnalyzeMarket(enrichedTick, currentSide)
	if !triggered {
		return
	}

	// 3. Money Management Decision Gate
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if decision == "GO_LONG" && pos.NetQuantity == 0 {
		if exitTime, ok := rm.lastExitTime[symbol]; ok {
			if rawTick.Timestamp.Sub(exitTime) < 5*time.Second {
				return
			}
		}

		allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / rawTick.LastPrice))
		if allowedQty <= 0 {
			return
		}

		// ⚡ CRITICAL RISK INTERCEPTION: Predict round-trip taxes for this entry *before* submitting it
		predictedFees := PredictRoundTripCharges(allowedQty, rawTick.LastPrice)

		// Evaluate if committing to this trade's fees will push us past our daily loss boundary
		projectedNetSessionPnL := rm.dailyRealized - (rm.dailyChargesPaid + predictedFees)
		if projectedNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Warnf("[Money Manager] Vetoed Scalper Signal for %s. Predicted tax drag (₹%.2f) breaches remaining daily risk wallet.", symbol, predictedFees)
			return // Order Restrained!
		}

		orderReq := models.OrderRequest{
			Symbol:          symbol,
			Product:         "MIS",
			TransactionType: "BUY",
			OrderType:       "MARKET",
			Quantity:        allowedQty,
			UserEmail:       AgentEmail,
		}

		logger.Infof("[Money Manager] Approved Setup. Predicted tax buffer: ₹%.2f. Executing BUY for %s", predictedFees, symbol)

		// Instantly lock fees into our accounting book
		rm.dailyChargesPaid += predictedFees

		pos.NetQuantity = allowedQty
		pos.Side = "LONG"
		pos.AveragePrice = rawTick.LastPrice
		pos.UnrealizedPnL = 0.0

		_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)

	} else if decision == "EXIT_LONG" && pos.NetQuantity != 0 {
		rm.executeBrokerOrder(symbol, pos, "Scalper Signal Exit", rawTick.Timestamp)
	}
}

func (rm *RiskManager) executeBrokerOrder(symbol string, pos *models.Position, reason string, timestamp time.Time) {
	if pos.NetQuantity == 0 {
		return
	}

	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	exitReq := models.OrderRequest{
		Symbol:          pos.Symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        pos.NetQuantity,
		UserEmail:       AgentEmail,
	}

	logger.Warnf("[Money Manager] Executing Square-Off for %s. Reason: %s", symbol, reason)

	rm.lastExitTime[symbol] = timestamp
	rm.dailyRealized += pos.UnrealizedPnL

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

	_, _ = rm.orderManager.PlaceOrder(context.Background(), exitReq)
}
