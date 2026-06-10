# Strategic Specifications: Institutional Ledger Strategy

**Logical Model:** Trend Acceptance & Capital Commitment Tracking

**Asset Classes:** Equities / Intraday High-Beta Liquid Assets

---

## 1. Core Paradigm Shift

Intraday opening volatility (09:15 AM – 09:30 AM) is fundamentally dominated by high retail participation, algorithm matching noise, and pre-market order clearing. Attempting to catch directional drives during this chaotic phase often yields lower win rates and subjects capital to sharp morning whipsaws (the "early morning rush traps").

The **Institutional Ledger Strategy** operates on a paradigm of patient trend confirmation. It remains strictly flat during opening liquidity spikes, treating the Volume-Weighted Average Price (VWAP) as the absolute session anchor line. Rather than predicting direction, it waits for institutional players to signal their intention through sustained price acceptance and high-conviction order flow. Once a dominant side establishes control, the system uses an absolute volume accounting framework to discover low-risk pullback entries and protect active capital against hidden distribution or trend wipeouts.

---

## 2. Core Architectural Pillars

### A. Session Landscape Context (The Morning Opening Vector)

At exactly 09:15 AM, the system takes a structural freeze-frame of the market open. By evaluating the initial print against historical closing boundaries and the opening VWAP placement, it establishes the day's macro structural vector:

* **Gap Up Session Vector:** Filtered exclusively to scan for long trend acceptance setups above VWAP.
* **Gap Down Session Vector:** Filtered exclusively to scan for short trend acceptance setups below VWAP.

This fixed structural gate prevents the engine from over-trading or getting trapped in mean-reverting chop on neutral or conflicting days.

### B. Time-at-Price VWAP Acceptance

To differentiate a true structural breakout from a localized stop-hunting trap, the system enforces a strict mathematical verification rule known as **Time-at-Price**.

* **The Validation Gate:** A stock is not confirmed to be in a trend simply because it crosses VWAP. It must print a minimum of **3 consecutive, closed 1-minute candle bodies** completely on the breakout side of the VWAP line.
* **The Mechanics:** This filter relies on the reality that localized noise and retail-driven spikes typically fail and snap back within 60 to 120 seconds. Institutional order blocks require sustained execution time, leaving a footprint of consecutive closed bars that establishes structural acceptance.

### C. The Institutional Volume Effectiveness Ledger

Tracking raw price or relative volume percentiles (like P90/P97 ranks) in isolation can mislead an algorithm. A high relative volume rank during low-liquidity mid-day zones represents far less absolute capital commitment than a standard bar during a high-liquidity morning drive.

To overcome this, the strategy maintains a real-time session balance sheet tracking absolute volume commitment, credited to two opposing teams based on **Volume Effectiveness**:

1. **Bullish Push Bucket (`BullishPushVolume`):** Accumulated raw absolute volume from bars exhibiting heavy relative volume intensity ($\ge \text{Rank } 5$) AND clear positive structural price body expansion ($\ge \text{Rank } 4$) in a bullish direction.
2. **Bearish Push Bucket (`BearishPushVolume`):** Accumulated raw absolute volume from bars exhibiting heavy relative volume intensity ($\ge \text{Rank } 5$) AND clear negative structural price body expansion ($\ge \text{Rank } 4$) in a bearish direction.

This ledger acts as an un-gameable accounting ledger of institutional capital commitment.

---

## 3. Microstructural Execution Mechanics

### A. FLAT Phase: Entry Discovery Logic

Once `IsVwapAcceptanceConfirmed` flips to true, the execution engine switches from a passive monitor to an active hunter, looking for two high-probability structural footprints:

#### 1. Setup Variant A: The Patient Pullback (Mean Reversion Entry)

* **Condition:** The stock has established a clear structural vector (e.g., Gap Up + accepted above VWAP), and its volume ledger shows buyers completely dominate sellers (Counter-force volume ratio $< 30\%$).
* **Execution Trigger:** The strategy avoids chasing the breakout high. It waits for the price to calmly mean-revert directly back down to the VWAP line, entering within a tight $0.15\%$ buffer envelope.
* **Validation Footprint:** The pullback must print low relative volume ($\le \text{Rank } 4$). This proves that the retracement is caused by a temporary lack of active buying rather than aggressive institutional selling.

#### 2. Setup Variant B: Aggressive Runaway Protection (Momentum Breakout Entry)

* **Condition:** Certain high-alpha institutional institutional movers spike aggressively out of the gate (e.g., up $2\%$ in minutes) and refuse to pull back to the VWAP anchor line, eventually closing up $5-6\%$ by session end.
* **Execution Trigger:** If a stock has confirmed clear acceptance and its volume ledger is heavily dominated by buyers, the appearance of a fresh institutional breakout candle—characterized by a sudden burst of high volume ($\ge \text{Rank } 6$) matched with a large green candle body ($\ge \text{Rank } 5$)—will trigger an immediate long entry to capture the runaway trend.

---

### B. ACTIVE Phase: Risk Management & Position Exits

Once inside a trade, the engine actively protects capital using two distinct layers of invalidation:

#### 1. Structural Price Invalidation

If the price breaks cleanly through the VWAP anchor line and closes past twice the entry buffer zone (e.g., $0.30\%$ beyond VWAP), the structural floor has cracked. The execution thesis is invalidated, and the position is closed.

#### 2. Volume Ledger Wipeout Protection (The Balance Sheet Guard)

This is the system’s defense against sudden trend reversals or hidden institutional distribution. If the strategy is long and has accumulated substantial bullish push volume, it continuously measures incoming counter-volume.

* **The Rule:** If a wave of aggressive institutional selling hits the book and prints red expansion bars whose absolute volume wipes out $60\%$ or more of the accumulated bullish setup volume, the balance sheet has hit a critical deficit.
* **The Action:** The engine executes an immediate market exit. It does not wait for a price-based stop loss or trailing lock to get hit; it recognizes that the large institutional buyers who originally defended the trend have either stopped participating or are being overwhelmed by a larger seller.

---

### C. Intelligent Trailing Profit Allocation

To extract maximum gains from major trend days without getting prematurely shaken out during normal mid-day consolidations (the "slow downs"), the strategy features a dynamic trailing leash governed by the institutional domination ratio:

* **Absolute Domination Track (Selling Ratio $< 15\%$):** If institutional buying volume completely dwarfs selling volume, the big players are actively driving the book and defending pullbacks. The strategy automatically widens its trailing leash, requiring a $70\%$ collapse from the trade's peak extension (`PeakVwapExtension`) before triggering a profit lock. This gives the asset ample room to breathe through mid-day lulls and ride the wave to session close.
* **Contested Ledger Track (Selling Ratio $\ge 15\%$):** If the opposing volume bucket begins making incremental gains, it indicates an active tug-of-war. The strategy automatically tightens its leash, executing an intelligent profit lock if $40\%$ of the peak extension evaporates, preserving accumulated gains before a deeper structural rollover occurs.

---

## 4. Operational State Mapping (Live Data Ledger Lifecycle)

The table below traces how the simplified `InstrumentState` engine metrics update across a standard trading day session layout:

```
[09:15 AM Market Open] 
         │
         ▼
[Compute Gap Vector] ──► (Sets IsGapUp or IsGapDown based on initial print)
         │
         ▼
[Monitor VWAP Anchor] ──► (Counts consecutive 1m candle body closes above/below VWAP)
         │
         ▼
[3 Closes Achieved] ──► (IsVwapAcceptanceConfirmed flips to TRUE)
         │
         ▼
[Evaluate Volume Ledger] ──► (Is counter-force ratio < 30%?)
         │
         ┌───────────────┴───────────────┐
         ▼                               ▼
[Variant A: Low-Vol Pullback]   [Variant B: High-Vol Breakout]
         │                               │
         └───────────────┬───────────────┘
                         ▼
               [Fire Market Entry]
                         │
                         ▼
            [Active Position Tracking]
   ┌─────────────────────┼─────────────────────┐
   ▼                     ▼                     ▼
[Price Crosses VWAP]  [Ledger Deficit >= 60%] [Domination Trend Stalls]
   │                     │                     │
   ▼                     ▼                     ▼
(Price Invalidation)  (Wipeout Protection)  (Intelligent Profit Lock)

```

1. **Context Initialization (09:15 AM):** `InitialOpenPrice` captures the first printed value. `IsGapUp` or `IsGapDown` locks in and provides a fixed directional bias for the rest of the day.
2. **Acceptance Phase (09:16 AM – 09:45 AM):** Price fluctuates across VWAP. The counters `ConsecutiveClosesAboveVwap` and `ConsecutiveClosesBelowVwap` continually increment and reset. No entries are permitted.
3. **Ledger Accumulation (09:46 AM):** A massive institutional candle cuts through VWAP. `BullishPushVolume` increments by the bar's absolute volume because its analytics meet the required `VolumeRank` and `PriceRank` thresholds.
4. **Confirmation Milestone (09:49 AM):** The third consecutive bar closes above VWAP. `IsVwapAcceptanceConfirmed` transitions to `true`.
5. **Execution Trigger (10:02 AM):** Price retraces cleanly to VWAP on a low volume rank bar ($\le 4$). Distance matches the buffer, the counter-force ratio is clear, and a `GO_LONG` market order fires.
6. **Dynamic Protection Tracking (11:30 AM):** Price hits a mid-day slow down and trends sideways. Opposing volume stays flat, keeping the counter-force ratio below $15\%$. The trailing profit lock widens to maximize ride-time.
7. **Wipeout Guard or Profit Lock (02:15 PM):** Either a heavy distribution wave hits the book—tripping the $60\%$ ledger deficit rule and triggering a defensive market exit—or the trend stalls and decays, crossing the $30\%$ peak extension threshold to trigger an `INTELLIGENT_PROFIT_LOCK`.