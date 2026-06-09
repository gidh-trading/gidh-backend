CREATE TABLE IF NOT EXISTS strategy_optimization_logs
(
    id                  BIGSERIAL PRIMARY KEY,
    symbol              VARCHAR(50)      NOT NULL,
    strategy_name       VARCHAR(100)     NOT NULL,
    trade_side          VARCHAR(10)      NOT NULL,
    minutes_since_open  INT              NOT NULL,

    -- 📊 The Microstructural Entry Snapshot Fields
    entry_timestamp     TIMESTAMPTZ      NOT NULL,
    entry_price         DOUBLE PRECISION NOT NULL,
    entry_vwap          DOUBLE PRECISION NOT NULL,
    entry_volume_rank   INT              NOT NULL,
    entry_price_rank    INT              NOT NULL,
    entry_wick_ratio    DOUBLE PRECISION NOT NULL,
    entry_vwap_distance DOUBLE PRECISION NOT NULL,

    -- 🎯 Outcome Fields
    exit_timestamp      TIMESTAMPTZ,
    exit_price          DOUBLE PRECISION,
    exit_reason         VARCHAR(50),
    final_pnl_inr       DOUBLE PRECISION DEFAULT 0 NOT NULL,
    peak_pnl_inr        DOUBLE PRECISION DEFAULT 0 NOT NULL,

    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_strat_opt_symbol ON strategy_optimization_logs (symbol);
CREATE INDEX IF NOT EXISTS idx_strat_opt_name ON strategy_optimization_logs (strategy_name);