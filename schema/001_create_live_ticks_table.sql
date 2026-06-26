CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =========================================================================
-- 1. LIVE TICKS TABLE (Raw Stream)
-- =========================================================================
CREATE TABLE IF NOT EXISTS live_ticks
(
    timestamp            TIMESTAMPTZ NOT NULL,
    instrument_token     INTEGER     NOT NULL,
    stock_name           TEXT,
    last_price           DOUBLE PRECISION,
    last_traded_quantity BIGINT,
    average_traded_price DOUBLE PRECISION,
    volume_traded        BIGINT,
    total_buy_quantity   BIGINT,
    total_sell_quantity  BIGINT,
    open                 DOUBLE PRECISION,
    high                 DOUBLE PRECISION,
    low                  DOUBLE PRECISION,
    close                DOUBLE PRECISION,
    change               DOUBLE PRECISION
);

-- Convert to hypertable
SELECT create_hypertable('live_ticks', 'timestamp', if_not_exists => TRUE);

-- Configure Compression Settings
-- We segment by instrument_token so historical lookups for one stock are instant
ALTER TABLE live_ticks SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_token',
    timescaledb.compress_orderby = 'timestamp DESC'
    );

-- Turn on compression for chunks older than 7 days
SELECT add_compression_policy('live_ticks', INTERVAL '7 days', if_not_exists => TRUE);

-- Update automated rolling data retention window to 45 days
SELECT add_retention_policy('live_ticks', INTERVAL '45 days', if_not_exists => TRUE);

-- Indexes for active/recent row lookups
CREATE INDEX IF NOT EXISTS idx_live_ticks_token ON live_ticks (instrument_token, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_ticks_stock ON live_ticks (stock_name, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_live_ticks_stream
    ON live_ticks (timestamp ASC, instrument_token);

