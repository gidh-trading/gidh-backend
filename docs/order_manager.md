Let’s pause code implementation completely and focus entirely on creating a clean, rock-solid **Architectural Specification Document**. Laying out the precise logic states, database mappings, and recovery flows ensures that when we eventually write code, it will be robust and free of edge-case bugs.

Here is the functional specification detailing your new localized order management architecture.

---

# Architectural Specification: Localized Position Risk & Execution Engine

## 1. Core Architectural Philosophy

To reduce the complexity of exchange rate-limiting, race conditions, and asynchronous multi-leg synchronization (OCO/Bracket tracking), this engine completely separates **Initial Entry Routing** from **Continuous Position Risk Monitoring**.

* **Exchange Responsibility:** The external exchange (Zerodha Kite) is used exclusively as a transaction routing channel for singular `MARKET` and `LIMIT` orders.
* **Go Process Responsibility:** 100% of the Stop Loss (SL) and Take Profit (TP) trigger evaluations are moved into the Go backend application's memory loop. The application continuously scans the live tick stream and fires immediate, single `MARKET` liquidation orders when a threshold is breached.

---

## 2. Definitive Execution Matrix

The application supports exactly 4 atomic order types with strict execution paths:

1. **Market Orders (Entry or Manual Scale):** Routed directly to the exchange for immediate match. Once confirmed filled, it updates the stock's singular parent position box.
2. **Limit Orders (Entry or Passive Scale):** Posted to the exchange order book in a `PENDING` state.
3. **Stop Loss (Risk Rule):** Strictly a **Local Memory Condition** checked against every incoming tick. Triggers a standalone, immediate exchange **Market Order** for the full outstanding net quantity to close exposure.
4. **Take Profit (Risk Rule):** Strictly a **Local Memory Condition** checked against every incoming tick. Triggers a standalone, immediate exchange **Market Order** for the full outstanding net quantity to close exposure.

---

## 3. Database Schema Alignment

To ensure total crash resilience, the database requires only two cleanly decoupled tables. There are no OCO cross-link columns or multi-order pointer relationships.

### Table A: `gidh_orders` (The Historical Audit Ledger)

Records the absolute history of distinct placement tokens sent to or simulated for the asset.

```sql
CREATE TABLE IF NOT EXISTS gidh_orders (
    order_id     TEXT PRIMARY KEY,
    symbol       TEXT NOT NULL,
    product      TEXT NOT NULL, -- MIS, CNC
    side         TEXT NOT NULL, -- BUY, SELL
    order_type   TEXT NOT NULL, -- MARKET, LIMIT
    quantity     INTEGER NOT NULL,
    filled_qty   INTEGER DEFAULT 0,
    price        DOUBLE PRECISION, -- Target entry price for LIMIT orders
    status       TEXT NOT NULL, -- PENDING, COMPLETE, CANCELLED, REJECTED
    timestamp    TIMESTAMPTZ NOT NULL,
    user_email   TEXT
);

```

### Table B: `gidh_positions` (The Persistent Real-Time State Engine)

Tracks the aggregated risk coordinates for the active asset. This single row holds your cost baseline and your localized execution thresholds.

```sql
CREATE TABLE IF NOT EXISTS gidh_positions (
    trading_date    DATE NOT NULL,
    symbol          TEXT NOT NULL,
    product         TEXT NOT NULL, -- MIS, CNC
    side            TEXT NOT NULL, -- LONG, SHORT, FLAT
    net_quantity    INTEGER NOT NULL, -- Accumulates all executed order volume
    avg_price       DOUBLE PRECISION NOT NULL, -- Blended entry price average
    realized_pnl    DOUBLE PRECISION DEFAULT 0, -- Locked-in closed returns
    local_take_profit DOUBLE PRECISION DEFAULT 0, -- Local RAM trigger price
    local_stop_loss   DOUBLE PRECISION DEFAULT 0, -- Local RAM trigger price
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trading_date, symbol, product)
);

```

---

## 4. Modification Matrix

This new layout splits the modification handlers into two clean, independent API execution flows:

```
                  ┌───────────────────────────────┐
                  │ Frontend Modification Request │
                  └───────────────┬───────────────┘
                                  │
         ┌────────────────────────┴────────────────────────┐
         ▼                                                 ▼
[ Order ID Provided ]                             [ Symbol & Product Provided ]
         │                                                 │
         ▼ (Modify Entry Limit)                            ▼ (Modify Exit Targets)
 1. Mutate `price` on PENDING order               1. Mutate `local_sl` or `local_tp`
 2. Live: Shoot single exchange modify             2. Pure RAM change on Position map
 3. Paper: Update in-memory book row              3. Instantly active on NEXT tick
 4. Async update `gidh_orders` table              4. Async update `gidh_positions` table

```

### Flow A: Modifying an Entry Limit Order (`ModifyOrder`)

* **Scope:** Applies strictly to `PENDING` limit entries.
* **Action:** Modifies only the target entry price. It has no awareness of or connection to SL or TP thresholds.
* **Broker impact:** Fires a single, lightweight order modification request to the broker interface.

### Flow B: Modifying Take Profit / Stop Loss Boundaries (`UpdatePositionMetadata`)

* **Scope:** Applies directly to an active, accumulated `Position` box.
* **Action:** Overwrites the `local_take_profit` and `local_stop_loss` floating-point numbers inside the RAM position container.
* **Broker impact:** **Zero broker/exchange interaction.** The update is purely localized and saves exchange rate-limiting bandwidth. It is instantly active for evaluation on the very next incoming market tick.

---

## 5. App-Start State Reconstruction Engine (Crash Recovery)

If the server drops power, panics, or undergoes a mid-session update restart, it can completely restore its operational state using the database ledger without querying or scraping broker orders.

### The App-Start Reconstruction Algorithm:

1. **Initialize Core Maps:** On application launch, initialize empty memory allocations for `activePositions` and the pending `orderBook` cache.
2. **Hydrate Active Positions:**
* Query `gidh_positions` where `trading_date = CURRENT_DATE` and `net_quantity != 0`.
* Load every returned row directly into the in-memory `activePositions` tracking map.
* *Result:* The engine instantly regains awareness of exactly how many shares it holds, what the blended average cost is, and the exact local `local_stop_loss` and `local_take_profit` boundaries.


3. **Hydrate Pending Orders:**
* Query `gidh_orders` where `status = 'PENDING'` and `order_type = 'LIMIT'`.
* Load these records into the active memory order book loop.
* *Result:* The engine immediately restores its entry order book matching listeners.


4. **Live Exchange Verification Synchronization:**
* *Live Mode Only:* Cross-reference the reconstructed pending orders and net positions with a single broker check (`kiteClient.GetPositions()` and `GetOrders()`). If a limit order filled or was cancelled while the server was offline, the state is reconciled to match reality.


5. **Resume Continuous Stream Loop:** Kick off the stream manager worker pool. The very first tick processed immediately evaluates against the fully restored position boundaries and local thresholds.

---

### Discussion Points for Review:

1. Does this architecture match your blueprint?
2. If a limit order fills, should the engine automatically initialize the position with a default SL and TP (e.g., using a fixed offset or an ATR multiplier from your ML model), or should it wait for the user/model to explicitly push the targets via the metadata route?