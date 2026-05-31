# Architectural Specification: Microstructural Reinforcement Learning Environment

**Protocol Version:** 1.1.0

**Transport Engine:** Localhost gRPC (IPC Optimized via Unix Domain Sockets)

**System Topology:** Go Continuous Pipeline (Server) $\longleftrightarrow$ Python Gymnasium Model (Client)

---

## 1. System Philosophy & Continuous State Representation

To train a high-frequency microstructural scalper capable of executing trades across an asset-agnostic universe of 32 instruments, the system avoids traditional fixed time boundaries. Because your bars are rolling and refresh on every contract match, the environment acts as a fluid multi-timeframe state machine.

Rather than waiting for candle boundary periods to close, the Go processing engine continuously recalculates all rolling windows in RAM. The telemetry stream is divided into three distinct spaces to give the model immediate tactical triggers, smooth macro context trends, and clear structural coordinate boundaries.

---

## 2. Telemetry Layer Specifications

### Dimension A: Continuous Micro-Triggers (60-Second Inflow)

This dimension captures instantaneous auction energy over a rolling 60-second window. It functions as the immediate signal telling the agent *when* to execute.

* **Circadian Mapped Ranks (`tick_volume_rank`, `tick_tick_rank`):** The Go processing pipeline reads raw trade messages and scales recent velocity against time-of-day statistical baselines. This step converts volatile raw numbers into stationary integers from 1 to 7.
* **Price & Range Momentum Ranks (`tick_price_rank`, `tick_range_rank`):** These track the immediate price direction and variance within the active 60-second window, flagging micro-breakouts or localized absorption.
* **Committed Order Book Intent (`order_book_imbalance`):** To avoid passing unstable raw volume numbers, the system calculates a bounded ratio from top-of-book quantities:

$$\text{order\_book\_imbalance} = \frac{\text{total\_buy\_qty} - \text{total\_sell\_qty}}{\text{total\_buy\_qty} + \text{total\_sell\_qty}}$$



This formula yields a continuous scale floating strictly between $-1.0$ (complete sell-side dominance) and $+1.0$ (complete buy-side dominance).

### Dimension B: Multi-Timeframe Rolling Context (1m to 15m)

Because your bars update on every tick, they do not represent static history; instead, they function as **multi-layered smoothing filters** that show the broader velocity of the market. The agent reads these to understand if a brief 60-second volume spike aligns with a larger, sustained momentum wave.

* **`change_pct`:** Captures the continuous direction and trend of the rolling timeframe.
* **`peak_volume_rank` & `peak_price_rank`:** These record the maximum volume and price intensity reached during the active rolling window's lifespan, helping the agent differentiate between thin-book slippage and institutional momentum.

### Dimension C: Volatility-Normalized Auction Geometry

The environment tracks price location relative to major intraday volume concentrations. To ensure the network weights remain effective across assets with vastly different share values, absolute price differences are divided by the asset's active 14-day Average True Range (`atr_14`).

$$\text{dist\_to\_vah} = \frac{\text{Last Price} - \text{Volume Profile VAH}}{\text{ATR}_{14}}$$

$$\text{dist\_to\_vwap} = \frac{\text{Last Price} - \text{Rolling VWAP}}{\text{ATR}_{14}}$$

If `dist_to_vah` resolves to $+0.5$, the model understands that the price has advanced exactly half an ATR unit above the value area high, standardizing spatial awareness whether trading a low-priced or high-priced stock.

---

## 3. Production Protobuf Implementation Schema

This optimized Interface Definition Language (IDL) file handles binary serialization, removing processing bottlenecks to sustain your target $50,000\text{x}$ backtesting speed factor over localhost connections.

```protobuf
syntax = "proto3";

package gidh;
option go_package = "internal/service/grpc;gidhgrpc";

// ScalperTrainingEngine orchestrates the high-speed data loop between
// the Go backtest source and the Python Gymnasium environment.
service ScalperTrainingEngine {
  // Server-streaming RPC: Continuously streams telemetry frames on every single tick
  rpc StreamTelemetry (BacktestSessionRequest) returns (stream MarketTickPacket);
}

message BacktestSessionRequest {
  string date = 1;
  string stock_name = 2;
}

message MarketTickPacket {
  // ---- 1. Telemetry Headers ----
  int64 timestamp_ms = 1;
  uint32 instrument_token = 2;
  double last_price = 3;
  double atr_14 = 4;                 // Anchor unit used to normalize rewards and geometry

  // ---- 2. Sliding 60s Buffer Ranks (Micro Triggers) ----
  int32 tick_volume_rank = 5;        // Mapped via in-memory baseline tables
  int32 tick_tick_rank = 6;          // Mapped via in-memory baseline tables
  int32 tick_price_rank = 7;         // Continuous rolling window body rank
  int32 tick_range_rank = 8;         // Continuous rolling window height rank
  double order_book_imbalance = 9;   // Scaled order depth ratio bounded between [-1.0, 1.0]

  // ---- 3. Multi-Timeframe Context Alignment (Tick-Updated Rolling Footprints) ----
  RollingBarTelemetry rolling_bar_1m = 10;
  RollingBarTelemetry rolling_bar_3m = 11;
  RollingBarTelemetry rolling_bar_5m = 12;
  RollingBarTelemetry rolling_bar_10m = 13;
  RollingBarTelemetry rolling_bar_15m = 14;

  // ---- 4. Volatility-Normalized Auction Geometry (Coordinates / ATR14) ----
  double dist_to_vwap = 15;
  double dist_to_poc = 16;
  double dist_to_vah = 17;
  double dist_to_val = 18;

  // ---- 5. Paper Position Manager Synchronization Layers ----
  int32 active_position_side = 19;   // Active inventory state: 0 = Flat, 1 = Long, -1 = Short
  double current_unrealized_pnl = 20; // Extracted to calculate open risk adjustments
  double current_realized_pnl = 21;   // Extracted to calculate locked-in target loops
  
  bool is_session_eof = 22;          // Triggers Gymnasium environment truncation flags
}

message RollingBarTelemetry {
  double change_pct = 1;              // Continuous percent return of the rolling window
  int32 peak_volume_rank = 2;        // Maximum volume intensity reached during the window
  int32 peak_price_rank = 3;         // Price velocity rank at current tick evaluation
}

```

---

## 4. Environment State Machine Design

The client-side interface unwraps incoming binary payloads and maps them directly to the continuous matrix dimensions required by Gymnasium.

```
[Incoming gRPC Packet] ────► [Extract Arrays & Scales] ────► [Construct Gymnasium Observation Tensor]
                                                                        │
                                                                        ▼
                                                             Shape: (22,) Float32 Box

```

### Observation Space Mapping

The state vector parses the structured input data into a single flattened array of size `(22,)`:

```python
# Sequential array shape construction inside the Custom Gym Env:
observation_space = spaces.Box(
    low=np.array([
        1, 1, 1, 1, -1.0,                                           # Micro Indicators
        -1.0, 1, 1, -1.0, 1, 1, -1.0, 1, 1, -1.0, 1, 1, -1.0, 1, 1, # Multi-Timeframe Rolling Windows
        -20.0, -20.0, -20.0, -20.0,                                 # Auction Geometry Offsets
        -1, -50.0                                                   # Account Position Metrics
    ]),
    high=np.array([
        7, 7, 7, 7, 1.0,                                            # Micro Indicators
        1.0, 7, 7, 1.0, 7, 7, 1.0, 7, 7, 1.0, 7, 7, 1.0, 7, 7,      # Multi-Timeframe Rolling Windows
        20.0, 20.0, 20.0, 20.0,                                     # Auction Geometry Offsets
        1, 50.0                                                     # Account Position Metrics
    ]),
    dtype=np.float32
)

```

### Action Space Mapping

The environment enforces a discrete execution map matching the operational capabilities of your internal paper trading engine:

* `Action 0`: **Hold / Do Nothing** (Maintain existing open inventory or sit flat).
* `Action 1`: **Market Buy Entry** (Issue immediate long routing to execution loop).
* `Action 2`: **Market Short Entry** (Issue immediate sell/short routing to execution loop).
* `Action 3`: **Emergency Flatten** (Liquidate open inventory immediately at the active tick price).

---

## 5. Volatility-Adjusted Reward Engine Architecture

To train a responsive scalper that prioritizes capital protection and avoids holding risks through volatile cycles, the step reward function combines realized milestones with adjustments for changes in open equity risk. Both components are normalized against asset-specific volatility parameters.

### Step Reward Formula

$$\text{Step Reward}_t = \left( \frac{\text{Realized PnL}_t - \text{Realized PnL}_{t-1}}{\text{ATR}_{14}} \right) + \left( \gamma \times \frac{\Delta \text{Unrealized PnL}_t}{\text{ATR}_{14}} \right) - \text{Friction Penalty}$$

* **Realized Milestone Component:** Provides primary positive feedback only when the model closes a position and locks in profit. This deters the agent from letting profitable trades reverse into losses.
* **Unrealized Risk Component ($\gamma = 0.1$):** Provides small, incremental updates while a position is open, encouraging the model to let profits run toward targets and cut losses before hitting stop limits.
* **Friction Penalty:** A deduction applied specifically to entry choices (`Action 1` or `Action 2`) to simulate exchange transaction costs and slippage. This deters the model from over-trading on small price ripples.

### Termination and Drawdown Boundaries

The environment enforces strict parameter controls to end an episode early if risk limits are breached:

* **Drawdown Cap:** If the total loss on an active position drops below a predefined volatility multiplier (e.g., $-5.0 \times \text{ATR}_{14}$), the system triggers an emergency freeze (`terminated = True`) and applies a penalty score to the network weights.
* **Session EOF:** When the Go stream reaches the final row of a historical data block, it sets the `is_session_eof` flag to `true`, signaling the Gymnasium loop to complete the episode cleanly (`truncated = True`).