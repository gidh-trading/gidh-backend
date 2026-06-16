DROP TABLE IF EXISTS strategy_optimization_logs CASCADE;

CREATE TABLE IF NOT EXISTS strategy_optimization_logs
(
    id                          BIGSERIAL PRIMARY KEY,
    symbol                      VARCHAR(50)      NOT NULL,
    strategy_name               VARCHAR(100)     NOT NULL,
    trade_side                  VARCHAR(10)      NOT NULL,
    minutes_since_open          INT              NOT NULL,

    -- 📊 Snapshot fields matching your Go struct exactly
    entry_timestamp             TIMESTAMPTZ      NOT NULL,
    entry_price                 DOUBLE PRECISION NOT NULL,
    entry_vwap                  DOUBLE PRECISION NOT NULL,
    entry_volume_rank           INT              NOT NULL, -- Maps to EntryVolumeRank
    entry_price_rank            INT              NOT NULL, -- Maps to EntryPriceRank

    entry_efficiency            DOUBLE PRECISION NOT NULL, -- Maps to EntryEfficiency
    entry_delta                 DOUBLE PRECISION NOT NULL, -- Maps to EntryDelta
    entry_slope                 DOUBLE PRECISION NOT NULL, -- Maps to EntrySlope
    entry_vwap_distance         DOUBLE PRECISION NOT NULL, -- Maps to EntryVwapDistance

    -- 🎯 Outcome Metrics
    exit_timestamp              TIMESTAMPTZ,
    exit_price                  DOUBLE PRECISION,
    exit_reason                 VARCHAR(100),
    final_pnl_inr               DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    peak_pnl_inr                DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    efficiency_capture_ratio    DOUBLE PRECISION NOT NULL DEFAULT 0.0,

    created_at                  TIMESTAMPTZ      DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strat_opt_symbol ON strategy_optimization_logs (symbol);
CREATE INDEX IF NOT EXISTS idx_strat_opt_name   ON strategy_optimization_logs (strategy_name);