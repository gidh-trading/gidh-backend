-- Create a dedicated schema to isolate training memory tracking from live trading tables
CREATE SCHEMA IF NOT EXISTS gidh_ml_core;

-- Create the persistent memory registry table
CREATE TABLE IF NOT EXISTS gidh_ml_core.agent_memory_ledger
(
    stock_name      VARCHAR(20) NOT NULL,
    trading_date    DATE        NOT NULL,
    times_rehearsed INT         DEFAULT 1,
    last_value_loss NUMERIC     DEFAULT 0.0,
    priority_score  NUMERIC     DEFAULT 1.0, -- Scaled between 0.0 (Mastered) and 1.0 (High Priority)
    last_updated    TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (stock_name, trading_date)
);

-- Index the priority scores so our sampling engine queries can run instantly
CREATE INDEX IF NOT EXISTS idx_ml_memory_priority
    ON gidh_ml_core.agent_memory_ledger (priority_score DESC, trading_date DESC);


-- Create a clean sub-schema namespace for machine learning structures
CREATE SCHEMA IF NOT EXISTS gidh_ml_core;

CREATE TABLE IF NOT EXISTS gidh_ml_core.historical_features
(
    timestamp          TIMESTAMPTZ      NOT NULL,
    stock_name         TEXT             NOT NULL,
    tick_index         INTEGER          NOT NULL,
    last_price         DOUBLE PRECISION NOT NULL,
    atr_14             DOUBLE PRECISION NOT NULL,
    observation_vector REAL[]           NOT NULL
);

-- Convert to a hypertable partitioned by time (timestamp)
SELECT create_hypertable('gidh_ml_core.historical_features', 'timestamp', if_not_exists => TRUE);

-- Compound index to maximize fast block-reads in your Python dataset loader
CREATE INDEX IF NOT EXISTS idx_ml_features_lookup
    ON gidh_ml_core.historical_features (stock_name, timestamp ASC);

-- Turn on compression to minimize disk space over millions of historical ticks
ALTER TABLE gidh_ml_core.historical_features
    SET (
        timescaledb.compress,
        timescaledb.compress_segmentby = 'stock_name'
        );

-- Compress data older than 2 days automatically
SELECT add_compression_policy('gidh_ml_core.historical_features', INTERVAL '2 days', if_not_exists => TRUE);