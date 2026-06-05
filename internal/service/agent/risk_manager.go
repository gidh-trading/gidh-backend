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

	// Money Management Team additions: Track explicit fee overheads
	dailyChargesPaid float64
	globalSummary    models.ItemizedCharges
	executedTrades   []models.BacktestExecutedTrade
}

func NewRiskManager(om order.PositionManager, sa *ScalperAgent) *RiskManager {
	return &RiskManager{
		orderManager:     om,
		scalper:          sa,
		agentPositions:   make(map[string]*models.Position),
		lastExitTime:     make(map[string]time.Time),
		dailyRealized:    0.0,
		dailyChargesPaid: 0.0,
		circuitBroken:    false,
		executedTrades:   make([]models.BacktestExecutedTrade, 0),
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

	if pos.NetQuantity != 0 {
		multiplier := 1.0
		if pos.Side == "SHORT" {
			multiplier = -1.0
		}
		pos.UnrealizedPnL = (rawTick.LastPrice - pos.AveragePrice) * float64(pos.NetQuantity) * multiplier

		totalNetSessionPnL := rm.dailyRealized + pos.UnrealizedPnL - rm.dailyChargesPaid

		if totalNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Errorf("[Money Manager] True Capital Drawdown Breached (₹%.2f). Freezing Agent.", totalNetSessionPnL)
			rm.circuitBroken = true
			rm.executeBrokerOrder(symbol, pos, "Net Session Risk Floor Breach", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}

		loc, _ := time.LoadLocation("Asia/Kolkata")
		if rawTick.Timestamp.In(loc).Hour() == 15 && rawTick.Timestamp.In(loc).Minute() >= 15 {
			rm.executeBrokerOrder(symbol, pos, "Intraday 15:15 Force Square-off", rawTick.Timestamp)
			rm.mu.Unlock()
			return
		}
	}

	currentSide := pos.Side
	rm.mu.Unlock()

	decision, triggered := rm.scalper.AnalyzeMarket(enrichedTick, currentSide)
	if !triggered {
		return
	}

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

		predictedFees := PredictRoundTripCharges(allowedQty, rawTick.LastPrice)
		projectedNetSessionPnL := rm.dailyRealized - (rm.dailyChargesPaid + predictedFees)
		if projectedNetSessionPnL <= -MaxDailyLossAllowed {
			logger.Warnf("[Money Manager] Vetoed Scalper Signal for %s. Predicted tax drag (₹%.2f) breaches remaining daily risk wallet.", symbol, predictedFees)
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

		logger.Infof("[Money Manager] Approved Setup. Predicted tax buffer: ₹%.2f. Executing BUY for %s", predictedFees, symbol)

		// ⚡ Itemized Accounting Ledger Updates
		charges := computeItemizedCharges(allowedQty, rawTick.LastPrice)
		rm.globalSummary.Brokerage += charges.Brokerage
		rm.globalSummary.STT += charges.STT
		rm.globalSummary.StampDuty += charges.StampDuty
		rm.globalSummary.ExchangeTurnoverCharge += charges.ExchangeTurnoverCharge
		rm.globalSummary.SebiTurnoverCharge += charges.SebiTurnoverCharge
		rm.globalSummary.GST += charges.GST
		rm.globalSummary.TotalCharges += charges.TotalCharges

		rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
			Timestamp:       rawTick.Timestamp,
			Side:            "BUY",
			Symbol:          symbol,
			Exchange:        "NSE",
			Quantity:        allowedQty,
			AveragePrice:    rawTick.LastPrice,
			AllocatedCharge: charges.TotalCharges,
		})

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

	// ⚡ Record Sell/Exit Side itemized fees into our historical logging structure
	charges := computeItemizedCharges(pos.NetQuantity, pos.AveragePrice)
	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            exitSide,
		Symbol:          symbol,
		Exchange:        "NSE",
		Quantity:        pos.NetQuantity,
		AveragePrice:    pos.AveragePrice, // Captures entry-basis baseline calculation context
		AllocatedCharge: charges.TotalCharges,
	})

	rm.lastExitTime[symbol] = timestamp
	rm.dailyRealized += pos.UnrealizedPnL

	pos.NetQuantity = 0
	pos.Side = "FLAT"
	pos.AveragePrice = 0.0
	pos.UnrealizedPnL = 0.0

	_, _ = rm.orderManager.PlaceOrder(context.Background(), exitReq)
}

func (rm *RiskManager) IngestClosedBar(bar *models.Bar) {
	if rm.scalper != nil {
		rm.scalper.IngestClosedBar(bar)
	}
}
