# GIDH - Statistical Participation Anomaly Engine

We are designing a market microstructure enrichment engine focused ONLY on statistically abnormal participation behavior using committed-capital data.

The system is NOT:

* predicting direction
* detecting smart money
* detecting icebergs directly
* classifying reversals/breakouts
* using unreliable L2 assumptions

The system IS:

* a statistical anomaly engine
* participation normalization framework
* market-state telemetry layer

---

# Core Philosophy

Only trust committed-capital data:

1. Executed volume
2. Execution count (ticks)
3. Realized market response

Avoid relying on:

* visible queue size
* order book imbalance
* spoofable liquidity
* hidden-order assumptions

L2 may later become a confidence overlay only.

---

# Current Architecture

## Layer 1 — Raw Tick Stream

Input:

* price
* cumulative volume
* timestamps
* executions

---

## Layer 2 — Rolling Live Window

Rolling 60-second aggregation:

* liveVolume
* liveTickCount
* rollingHigh
* rollingLow
* realizedRange

Avoid fixed candle boundaries.

Markets are continuous.

---

## Layer 3 — Historical DNA

DNA stores minute-index baselines ONLY for stable intraday variables:

* VolumeMean
* VolumeStd
* TickCountMean
* TickCountStd

Example:
10:31 AM historically has expected:

* participation
* execution activity

DNA DOES NOT currently normalize:

* price
* displacement
* volatility
* efficiency

because those are less stationary.

---

# Primary Enrichment Features

## VolumeZ

Measures abnormal committed participation.

Formula:

VolumeZ =
(liveVolume - expectedVolumeMean) / expectedVolumeStd

---

## TickCountZ

Measures abnormal execution activity.

Formula:

TickCountZ =
(liveTickCount - expectedTickMean) / expectedTickStd

Important:
TickCountZ does NOT imply urgency.
It only measures abnormal execution frequency/activity.

---

# RelativeVolume

Relative magnitude context only.

Not primary anomaly logic.

---

# Price / Movement Philosophy

Do NOT normalize raw price movement initially.

Reason:

* price response is regime-dependent
* less stationary
* more nonlinear than participation

Instead:
price is treated as a RESPONSE variable.

---

# Efficiency Concept

Experimental telemetry only.

Definition:

Efficiency =
realizedRange / liveVolume

Possible future variants:

* range / tick count
* displacement / participation

Efficiency currently:

* has NO anomaly thresholds
* is NOT DNA-normalized
* is NOT interpreted

Purpose currently:

* observation
* visualization
* feature research
* distribution analysis

---

# Important Conceptual Insight

The system currently answers ONLY:

"Is participation behavior statistically abnormal?"

NOT:

* why
* bullish/bearish
* absorption
* trend
* liquidation
* iceberg

Interpretation comes later.

---

# Current Recommended Outputs

1. VolumeZ
2. TickCountZ
3. RelativeVolume
4. RealizedRange
5. Efficiency

---

# Current Design Rules

## Enrichment layer must remain:

* objective
* statistical
* interpretation-free

Avoid terminology:

* urgency
* whale
* iceberg
* smart money
* absorption

Those belong to future interpretation layers.

---

# Current Research Direction

Participation appears more statistically stable than price.

Therefore:

* participation is DNA-normalized
* price response currently remains live telemetry

Future possibility:

* efficiency normalization if distributions prove stable enough

But NOT yet.

---

# Mental Model

Tomato stall outside Tesco analogy:

Historical DNA:
"What is normal tomato activity at this time?"

Live engine:
"What is happening right now?"

Enrichment:
"How statistically abnormal is current tomato participation?"

That is the current system abstraction.
