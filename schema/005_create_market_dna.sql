-- schema/005_create_market_dna.sql

CREATE TABLE IF NOT EXISTS gidh_market_dna
(
    instrument_token INTEGER NOT NULL,
    stock_name       TEXT    NOT NULL,
    trading_date     DATE    NOT NULL,
    poc_5d           DOUBLE PRECISION,
    vah_5d           DOUBLE PRECISION,
    val_5d           DOUBLE PRECISION,
    macro_hvns       JSONB,
    macro_lvns       JSONB,
    time_buckets     JSONB,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (instrument_token, trading_date) -- Satisfies TimescaleDB constraint (must include partitioning column)
);

-- 1. Transform table into a TimescaleDB hypertable partitioned by 'trading_date'
-- 'migrate_data => true' handles records safely if the table already contains data.
SELECT create_hypertable('gidh_market_dna', 'trading_date',
                         if_not_exists => TRUE,
                         migrate_data => TRUE);

-- 2. Performance Indices
-- TimescaleDB automatically creates a default index on the time column ('trading_date DESC').
-- We add an optimized compound index to handle fast multi-instrument queries by the Go Watchtower.
CREATE INDEX IF NOT EXISTS idx_dna_token_date
    ON gidh_market_dna (instrument_token, trading_date DESC);


CREATE TABLE IF NOT EXISTS gidh_raw_observations
(
    instrument_token INTEGER          NOT NULL,
    minute_index     INTEGER          NOT NULL,
    trading_date     DATE             NOT NULL,
    price            DOUBLE PRECISION NOT NULL,
    volume           DOUBLE PRECISION NOT NULL,
    tick_count       DOUBLE PRECISION NOT NULL,
    relative_volume  DOUBLE PRECISION NOT NULL,
    realized_range   DOUBLE PRECISION NOT NULL,
    efficiency       DOUBLE PRECISION NOT NULL,
    has_valid_ticks  BOOLEAN     DEFAULT false,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (instrument_token, minute_index, trading_date)
);

-- 1. Transform raw observation tracking engine ledger into a Hypertable
SELECT create_hypertable('gidh_raw_observations', 'trading_date',
                         if_not_exists => TRUE,
                         migrate_data => TRUE);

-- 2. Performance Indices for Window Analysis
-- This composite index speeds up 'process_instrument' when it grabs the 30-day lookback slice for a specific token
CREATE INDEX IF NOT EXISTS idx_raw_obs_lookup
    ON gidh_raw_observations (instrument_token, trading_date DESC, minute_index);


-- Automatically drop raw operational telemetry chunks older than 30 days
SELECT add_retention_policy('gidh_raw_observations', INTERVAL '30 days', if_not_exists => TRUE);

-- Automatically drop compiled target DNA blobs older than 15 days
SELECT add_retention_policy('gidh_market_dna', INTERVAL '15 days', if_not_exists => TRUE);