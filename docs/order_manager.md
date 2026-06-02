# Architectural Specification Document: Localized Position Risk & Execution Engine

## 1. Core Architectural Philosophy

To maximize throughput and eliminate the synchronization bottlenecks inherent in managing multi-leg exchange orders (e.g., OCO, bracket orders), this architecture completely decouples **Initial Entry Routing** from **Continuous Position Risk Monitoring**.

* **Exchange Layer (Broker):** Used strictly as a transactional routing channel for singular, independent `MARKET` and `LIMIT` orders. The broker remains entirely unaware of your exit targets.
* **Application Layer (Go Engine):** 100% of Stop Loss (SL) and Take Profit (TP) evaluation occurs in-memory within the Go backend application loop. The engine monitors the live tick stream against active position coordinates and fires immediate, single exchange `MARKET` liquidation orders when a threshold is breached.

---

## 2. Order Type & Lifecycle Matrix

The engine recognizes exactly 4 execution entities, categorized into two cleanly separated lifecycles:

### A. The Entry Routing Channel (Orders)

1. **Market Orders:** Placed directly via the broker API for immediate execution. Once confirmed filled, they modify the asset's parent position container.
2. **Limit Orders:** Sent to the broker book in a `PENDING` state. They contain no awareness of or connection to exit bounds. If modified via `ModifyOrder`, only their entry limit price is updated.

### B. The Local Risk Watchdog (Positions)

3. **Stop Loss (Local Rule):** A localized downside protective threshold assigned manually to an *already active position*. It acts as a defensive floor checked against the real-time tick stream.
4. **Take Profit (Local Rule):** A localized upside target threshold assigned manually to an *already active position*. It acts as a profit ceiling checked against the real-time tick stream.

---

## 3. Database Schema Alignment

To ensure complete crash resilience, state restoration is handled via two decoupled database tables. There are no relational foreign-key strings or cross-linked bracket order pointers.

### Table A: `gidh_orders` (The Historical Audit Ledger)

Records the absolute history of explicit placement tokens sent to or simulated at the exchange layer.

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

Tracks the aggregated risk coordinates for active assets. This row stores your current session volume cost baseline alongside your local floating-point RAM exit thresholds.

```sql
CREATE TABLE IF NOT EXISTS gidh_positions (
    trading_date      DATE NOT NULL,
    symbol            TEXT NOT NULL,
    product           TEXT NOT NULL, -- MIS, CNC
    side              TEXT NOT NULL, -- LONG, SHORT, FLAT
    net_quantity      INTEGER NOT NULL, -- Current net volume active in market
    avg_price         DOUBLE PRECISION NOT NULL, -- Blended mathematical entry average
    realized_pnl      DOUBLE PRECISION DEFAULT 0, -- Locked-in session closed returns
    local_take_profit DOUBLE PRECISION DEFAULT 0, -- Local RAM profit price trigger
    local_stop_loss   DOUBLE PRECISION DEFAULT 0, -- Local RAM risk floor price trigger
    updated_at        TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trading_date, symbol, product)
);

```

---

## 4. State Dependency & Execution Rules

### Rule 4.1: Risk Rule Isolation

* Take Profit and Stop Loss thresholds can **only exist and be applied when a position is active** (`net_quantity != 0`).
* PENDING Limit or Market orders carry zero risk parameters at the routing layer.

### Rule 4.2: Manual Target Initialization

* When an entry order transitions to `COMPLETE` and opens or scales a position, the local RAM fields `local_stop_loss` and `local_take_profit` strictly default to `0` (disabled).
* The engine remains passive, waiting for an explicit manual request via the `/api/positions/metadata` API endpoint before applying exit boundaries.

### Rule 4.3: Automated Lifecycle Cleanup

* When an active position's `net_quantity` is reduced to exactly `0` (Flat), any local tracking variables are completely erased in RAM and set to `0` in the database.
* If a position is fully reversed (e.g., transitioning from `LONG` to `SHORT` via a larger opposing fill), previous directional risk rules are automatically invalidated.

---

## 5. Partial Fill & Isolated Execution Mechanics

### Rule 5.1: Continuous Position Accumulation

* Active positions absorb partial fills dynamically. Every execution postback frame modifies the `net_quantity` and updates the `average_price` mathematically based on the exact incremental executed volume delta.
* Unexecuted remaining leaves belonging to the original entry limit order continue to sit safely on the exchange book as a `PENDING` limit order.

### Rule 5.2: Independent Order Truncation

* A user can explicitly cancel the unexecuted portion of a partial fill at any time by issuing a standard delete request to `/api/orders/cancel` passing the entry `order_id`.
* The broker cancels the remaining balance, finalizes the order status as `CANCELLED`, and leaves the current active position size locked exactly at its current filled amount.

### Rule 5.3: Risk Trigger Isolation

* When a local memory Stop Loss or Take Profit threshold is breached, the engine determines liquidation volume by pulling the live `net_quantity` of the position container at that exact millisecond.
* The engine will **NOT** automatically cancel or alter any trailing `PENDING` entry limit orders for that stock during a risk exit. If a trailing entry limit order fills later in the session, it will open a new unhedged position with risk limits defaulted to `0`.

---

## 6. App-Start State Reconstruction Engine (Crash Recovery)

If the server drops connection, panics, or restarts mid-session, it completely restores its operational state using the database ledger without scraping historical order arrays from the broker:

1. **Hydrate Active Positions:** On boot, query `gidh_positions` where `trading_date = CURRENT_DATE` and `net_quantity != 0`. Load these directly into the in-memory `activePositions` tracking map. The engine instantly regains awareness of its current shares, blended cost, and precise local `local_stop_loss` and `local_take_profit` bounds.
2. **Hydrate Pending Entries:** Query `gidh_orders` where `status = 'PENDING'` and `order_type = 'LIMIT'`. Load these into the active memory order book loop cache.
3. **Live Exchange Verification:** *Live Mode Only:* Cross-reference the reconstructed pending orders and net positions with a fast broker check (`kiteClient.GetPositions()` and `GetOrders()`). If a limit order filled or was manually altered while the server was offline, the internal state is reconciled to match reality.
4. **Resume Stream Evaluation:** Start the pipeline worker pool. The very first tick processed immediately evaluates against the fully restored position thresholds.