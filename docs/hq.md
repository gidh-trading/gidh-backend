# HEADQUARTERS (HQ) SYSTEM EVOLUTION DOCUMENT

## Architectural Shift Summary

The architectural footprint of the `gidh-backend` system transitioned from a tight coupling of data ingestion and presentation to a decoupled, state-continuous, high-intelligence command layer. By stripping the **Scout** of heuristic responsibilities, it became a pure sensor, while the **Headquarters (HQ)** became the centralized state machine for order flow classification, risk verification, and UI signaling.

```
[Phase 1: Ingestion Clutter]
Scout Sensor ───> Blind Alert Ingestion ───> Overwhelmed UI & Rigid Strategy

[Phase 2: Decoupled Continuous Matrix]
Scout Sensor ───(Raw Telemetry Burst)───> [ Headquarters (HQ) ] ───> System UI
                                                  │ (Committed Tape Calculus)
                                                  ▼
                                       Order Management Layer

```

---

## The 4-Stage System Journey

### Stage 1: The Dumb Sensor Formulation

The journey began by recognizing that a low-latency data collector should not handle edge-case execution evaluations. The **Scout** was stripped of tracking logic and assigned a singular focus: raw telemetry packet processing (e.g., streaming immediate velocity spikes). The responsibility of alerting the system UI or triggering order managers was handed entirely to the new intermediate command structure: the **HQ**.

### Stage 2: Timeframe Independence (Decoupling from Bars)

To avoid the mathematical pitfalls of lagging candlestick intervals (OHLC), the HQ was designed as a **time-continuous sequential tick-processing architecture**. Instead of waiting for a 1-minute or 10-minute bar structure to close, the HQ forks transactions directly from the ingestion pipeline at pure tick velocity. It holds the entire day’s raw transaction ledger directly within highly optimized in-memory Go slices, providing instant historical lookback capabilities without requiring heavy DB re-queries mid-run.

### Stage 3: Real committed vs. Perceived Intent Correction

A vital breakthrough in the system's design occurred when identifying true institutional direction. The system separated **perceived intent** (spoofable, passive Order Book Depth, which can be canceled instantly) from **real committed capital** (un-spoofable transactions printed on the tape). HQ's classification models were re-engineered to look exclusively at historical execution data to evaluate market direction.

### Stage 4: True Direction & Absorption Mathematical Identification

The core logic of HQ was formalized around two primary microstructural metrics calculated dynamically over a continuous rolling tick window:

* **True Direction via Volume-Weighted Price Delta ($VWP\Delta$):** Calculates the explicit net capital allocation pushing prices higher or lower.

$$VWP\Delta = \sum \left( \text{TickVolume} \times (\text{LastPrice} - \text{PrevPrice}) \right)$$


* **Absorption Identification via Capital Efficiency:** Measures structural progress against volume spent. If a volume spike occurs but efficiency collapses to near zero, HQ identifies that a passive institutional participant is absorbing market orders. This allows HQ to suppress false breakout alerts and identify institutional traps.

---

## Production Readiness Matrix

To guarantee crash-proof continuity across the 32-stock trading universe, a state persistence layer was deployed using TimescaleDB hypertable structures:

| System Layer | Implementation Focus | Operational Safeguard |
| --- | --- | --- |
| **In-Memory Ledger** | `[]models.TickData` sliding array allocation per instrument token. | Low-overhead memory footprints handling an entire session's high-resolution tick history. |
| **Persistence Database** | `gidh_hq_stock_states` automated partitioning table. | Prevents mid-session disruption by enabling lightning-fast state rehydration upon reboot. |
| **UI Control Pipeline** | Integrated WebSocket broadcast throttling (e.g., 15s cooldown filters). | Protects client terminal render cycles from being overwhelmed by volatile high-frequency tick streams. |