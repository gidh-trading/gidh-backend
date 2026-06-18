CREATE TABLE optimized_strategy_configs
(
    -- 🕒 The exact time/date when this optimization matrix was generated or deployed
    optimization_date         TIMESTAMP WITH TIME ZONE NOT NULL,

    stock_name                VARCHAR(20)              NOT NULL,
    entry_tf                  VARCHAR(5)               NOT NULL, -- e.g., '1m'

    -- 📊 Core Decoupled Entry Filters
    min_volume_rank           INT                      NOT NULL,
    min_price_rank            INT                      NOT NULL,
    min_tick_rank             INT                      NOT NULL,
    eff_scalp_threshold       REAL                     NOT NULL,
    direction_mode            VARCHAR(15)              NOT NULL, -- 'lenient' or 'strict'

    -- 🔌 Plug-and-Play Optional Filters (Stored explicitly, NULL if omitted)
    min_efficiency_slope      REAL,
    long_time_above_vwap_pct  REAL,
    short_time_above_vwap_pct REAL,

    -- 🎯 Target Exit Blueprints for the Order Manager / Backtest validation
    take_profit_points        REAL                     NOT NULL, -- Derived from avg_peak_profit
    stop_loss_points          REAL                     NOT NULL, -- Derived from avg_peak_loss

    -- 📈 Performance Statistics at the time of optimization
    profit_pain_ratio         REAL                     NOT NULL,
    signal_count              INT                      NOT NULL,

    -- The primary key constraint must include the partition time column in TimescaleDB
    PRIMARY KEY (optimization_date, stock_name, entry_tf, min_volume_rank, min_price_rank, eff_scalp_threshold)
);

-- Convert to a hypertable so it scales perfectly as you accumulate daily configurations
SELECT create_hypertable('optimized_strategy_configs', 'optimization_date');

-- Index for retrieving the most recent configuration for a live asset
CREATE INDEX idx_live_strategy_lookup
    ON optimized_strategy_configs (stock_name, entry_tf, optimization_date DESC);