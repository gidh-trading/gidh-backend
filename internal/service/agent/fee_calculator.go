package agent

import "math"

// PredictRoundTripCharges computes exactly how much a full buy-and-square-off sequence will cost
func PredictRoundTripCharges(quantity int, price float64) float64 {
	turnoverSingleLeg := float64(quantity) * price
	totalTurnoverRoundTrip := turnoverSingleLeg * 2.0

	// 1. Brokerage: 0.03% capped at ₹20 per leg execution
	buyBrokerage := math.Min(turnoverSingleLeg*0.0003, 20.0)
	sellBrokerage := math.Min(turnoverSingleLeg*0.0003, 20.0)
	totalBrokerage := buyBrokerage + sellBrokerage

	// 2. STT (Securities Transaction Tax): 0.025% evaluated on the SELL leg only
	stt := turnoverSingleLeg * 0.00025

	// 3. Exchange Turnover Charge (NSE Intraday): 0.00322% per leg
	exchangeFees := totalTurnoverRoundTrip * 0.0000322

	// 4. SEBI Turnover Fees: 0.0001% per leg
	sebiFees := totalTurnoverRoundTrip * 0.0000001

	// 5. Stamp Duty: 0.003% evaluated on the BUY leg only
	stampDuty := turnoverSingleLeg * 0.00003

	// 6. GST: 18% applied to Brokerage + Exchange Fees + SEBI Fees
	gst := (totalBrokerage + exchangeFees + sebiFees) * 0.18

	return totalBrokerage + stt + exchangeFees + sebiFees + stampDuty + gst
}
