-- =========================================================================
-- 2. LIVE ORDER DEPTH TABLE (Raw Stream)
-- =========================================================================
CREATE TABLE IF NOT EXISTS live_order_depth
(
    timestamp        TIMESTAMPTZ NOT NULL,
    instrument_token INTEGER     NOT NULL,
    stock_name       TEXT,
    side             TEXT CHECK (side IN ('buy', 'sell')),
    price            DOUBLE PRECISION,
    quantity         BIGINT,
    orders           INTEGER
);

-- Convert to hypertable
SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);

-- Configure Compression Settings
-- We segment by token and side to optimize order book depth reconstruction queries
ALTER TABLE live_order_depth SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_token, side',
    timescaledb.compress_orderby = 'timestamp DESC'
    );

-- Turn on compression for chunks older than 7 days
SELECT add_compression_policy('live_order_depth', INTERVAL '7 days', if_not_exists => TRUE);

-- Update automated rolling data retention window to 45 days
SELECT add_retention_policy('live_order_depth', INTERVAL '45 days', if_not_exists => TRUE);

-- Indexes for active/recent row lookups
CREATE INDEX IF NOT EXISTS idx_order_depth_token ON live_order_depth (instrument_token, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_depth_stock ON live_order_depth (stock_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_depth_side ON live_order_depth (side);
CREATE INDEX IF NOT EXISTS idx_order_depth_token_side ON live_order_depth (instrument_token, side, timestamp DESC);