package agent

import (
	"math"
	"time"

	"gidh-backend/internal/service/models"
	"gidh-backend/pkg/logger"
)

func (rm *RiskManager) CalculatePositionSizeAndFees(symbol string, lastPrice float64) (int, float64) {
	currentLiquidCapital := InitialCapital + rm.dailyRealized - rm.dailyChargesPaid
	if currentLiquidCapital <= 0 {
		logger.Errorf("[Accounting] Allocation blocked for %s. Zero or negative corporate liquid balance.", symbol)
		return 0, 0
	}

	totalAccountBuyingPower := currentLiquidCapital * MaxLeverage
	allowedCapitalForStock := totalAccountBuyingPower * MaxCapitalPerStockPct

	allowedQty := int(math.Floor(allowedCapitalForStock / lastPrice))
	if allowedQty <= 0 {
		logger.Warnf("[Accounting] Allocation sizing resulted in 0 units for %s. Cost per share outstrips limit allocation.", symbol)
		return 0, 0
	}

	predictedFees := rm.PredictRoundTripCharges(allowedQty, lastPrice)
	return allowedQty, predictedFees
}

func (rm *RiskManager) LogTransactionFees(qty int, price float64, txType string, predictedFees float64, timestamp time.Time) {
	charges := rm.CalculateItemizedCharges(qty, price)

	rm.globalSummary.Brokerage += charges.Brokerage
	rm.globalSummary.STT += charges.STT
	rm.globalSummary.StampDuty += charges.StampDuty
	rm.globalSummary.ExchangeTurnoverCharge += charges.ExchangeTurnoverCharge
	rm.globalSummary.SebiTurnoverCharge += charges.SebiTurnoverCharge
	rm.globalSummary.GST += charges.GST
	rm.globalSummary.TotalCharges += charges.TotalCharges

	rm.executedTrades = append(rm.executedTrades, models.BacktestExecutedTrade{
		Timestamp:       timestamp,
		Side:            txType,
		Symbol:          "NSE",
		Quantity:        qty,
		AveragePrice:    price,
		AllocatedCharge: charges.TotalCharges,
	})

	rm.dailyChargesPaid += predictedFees
}

func (rm *RiskManager) PredictRoundTripCharges(qty int, price float64) float64 {
	singleWay := rm.CalculateItemizedCharges(qty, price)
	return singleWay.TotalCharges * 2.0
}

func (rm *RiskManager) CalculateItemizedCharges(qty int, price float64) models.ItemizedCharges {
	turnover := float64(qty) * price

	brokerage := math.Min(20.0, turnover*0.0003)
	stt := turnover * 0.00025
	exchangeTurnover := turnover * 0.0000325
	sebiTurnover := turnover * 0.0000001
	stampDuty := turnover * 0.00003
	gst := (brokerage + exchangeTurnover + sebiTurnover) * 0.18

	total := brokerage + stt + exchangeTurnover + sebiTurnover + stampDuty + gst

	return models.ItemizedCharges{
		Brokerage:              brokerage,
		STT:                    stt,
		ExchangeTurnoverCharge: exchangeTurnover,
		SebiTurnoverCharge:     sebiTurnover,
		StampDuty:              stampDuty,
		GST:                    gst,
		TotalCharges:           total,
	}
}

// ========================================================================
// 📊 UI METRICS & REPORTING
// ========================================================================

type UIContractNotePayload struct {
	Summary models.ItemizedCharges         `json:"summary"`
	Trades  []models.BacktestExecutedTrade `json:"trades"`
}

func (rm *RiskManager) GetUIContractNote() UIContractNotePayload {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	trades := rm.executedTrades
	if trades == nil {
		trades = []models.BacktestExecutedTrade{}
	}

	return UIContractNotePayload{
		Summary: rm.globalSummary,
		Trades:  trades,
	}
}
