package agent

import (
	"context"
	"fmt"
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/internal/service/order"
	"gidh-backend/pkg/logger"
)

const (
	AgentEmail          = "bot.scalper@gidh.tech"
	InitialCapital      = 20000.0 // ₹20,000 base wallet
	MaxLeverage         = 5.0     // 5x Intraday MIS Leverage
	MaxDailyLossAllowed = 1000.0  // ₹1,000 (5%) portfolio drawdown circuit breaker
)

type RiskManager struct {
	orderManager   order.PositionManager
	scalper        *ScalperAgent
	agentPositions map[string]*models.Position
	dailyRealized  float64
	circuitBroken  bool
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:   om,
		scalper:        sa,
		agentPositions: make(map[string]*models.Position),
		dailyRealized:  0.0,
		circuitBroken:  false,
	}
}

func (rm *RiskManager) IngestClosedBar(bar *models.Bar) {
	rm.scalper.IngestClosedBar(bar)
}

// ProcessSequentialTick runs in-line on every single tick inside backtest data streams
func (rm *RiskManager) ProcessSequentialTick(enrichedTick *models.EnrichedTick) {
	if rm.circuitBroken {
		return
	}

	symbol := enrichedTick.Raw.StockName
	key := fmt.Sprintf("%s:MIS", symbol)

	// 1. Resolve isolated accounting state profile
	pos, exists := rm.agentPositions[key]
	if !exists {
		pos = &models.Position{Symbol: symbol, Product: "MIS", NetQuantity: 0, Side: "FLAT"}
		rm.agentPositions[key] = pos
	}

	// 2. Money Management: Guard open floating exposure bounds
	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (enrichedTick.Raw.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier

		// Circuit Breaker Rule
		if rm.dailyRealized+pos.UnrealizedPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Money Manager] Max Loss Breached (₹%.2f). Halting Backtest Agent Operations.", rm.dailyRealized+pos.UnrealizedPnL)
			rm.circuitBroken = true
			rm.executeOrder(symbol, pos, "Daily Drawdown Circuit Breaker Tripped")
			return
		}

		// Intraday 3:15 PM Square-off Rule
		loc, _ := time.LoadLocation("Asia/Kolkata")
		exchangeTime := enrichedTick.Raw.Timestamp.In(loc)
		if exchangeTime.Hour() == 15 && exchangeTime.Minute() >= 15 {
			rm.executeOrder(symbol, pos, "Time Boundary: Forcing Intraday MIS Square-off")
			return
		}

		// Dynamic Trailing Risk Protection Fallback (0.5% stop-loss threshold)
		if pos.Side == "LONG" && enrichedTick.Raw.LastPrice < (pos.AveragePrice*0.995) {
			rm.executeOrder(symbol, pos, "Money Protection: Hard Stop Loss Crossed")
			return
		}
	}

	// 3. Delegate to Analytics: Evaluate market metrics via Scalper
	decision, triggered := rm.scalper.AnalyzeMarket(enrichedTick, pos.Side)
	if !triggered {
		return
	}

	// 4. Money Management: Check sizing bounds and route approved entries
	if decision == "GO_LONG" && pos.NetQuantity == 0 {
		loc, _ := time.LoadLocation("Asia/Kolkata")
		if enrichedTick.Raw.Timestamp.In(loc).Hour() >= 15 {
			return // Reject entries past 3:00 PM IST
		}

		// Calculate exact allowed quantity based on 5x MIS buying bounds
		allowedQty := int(math.Floor((InitialCapital * MaxLeverage) / enrichedTick.Raw.LastPrice))
		if allowedQty <= 0 {
			return
		}

		orderReq := models.OrderRequest{
			Symbol:          symbol,
			Product:         "MIS",
			TransactionType: "BUY",
			OrderType:       "MARKET",
			Quantity:        allowedQty,
			UserEmail:       AgentEmail,
		}

		logger.Infof("[Money Manager] Approved Scalper Setup. Executing BUY for %s: Qty %d", symbol, allowedQty)

		pos.NetQuantity = allowedQty
		pos.Side = "LONG"
		pos.AveragePrice = enrichedTick.Raw.LastPrice
		pos.UnrealizedPnL = 0.0

		_, _ = rm.orderManager.PlaceOrder(context.Background(), orderReq)

	} else if decision == "EXIT_LONG" && pos.NetQuantity != 0 {
		rm.executeOrder(symbol, pos, "Scalper Analytics Exit Triggered")
	}
}

func (rm *RiskManager) executeOrder(symbol string, pos *models.Position, reason string) {
	if pos.NetQuantity == 0 {
		return
	}

	exitSide := "SELL"
	if pos.Side == "SHORT" {
		exitSide = "BUY"
	}

	exitReq := models.OrderRequest{
		Symbol:          symbol,
		Product:         "MIS",
		TransactionType: exitSide,
		OrderType:       "MARKET",
		Quantity:        pos.NetQuantity,
		UserEmail:       AgentEmail,
	}

	logger.Warnf("[Money Manager] Dispatching Exit Order for %s. Reason: %s", symbol, reason)

	rm.dailyRealized += pos.UnrealizedPnL
	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

	_, _ = rm.orderManager.PlaceOrder(context.Background(), exitReq)
}
