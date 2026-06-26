CREATE TABLE IF NOT EXISTS gidh_bars
(
    timestamp        TIMESTAMPTZ      NOT NULL,
    instrument_token INTEGER          NOT NULL,
    stock_name       TEXT             NOT NULL,
    timeframe        TEXT             NOT NULL,

    -- OHLC & Core Transaction Activity Data
    open             DOUBLE PRECISION NOT NULL,
    high             DOUBLE PRECISION NOT NULL,
    low              DOUBLE PRECISION NOT NULL,
    close            DOUBLE PRECISION NOT NULL,
    volume           DOUBLE PRECISION NOT NULL DEFAULT 0,
    tick_count       BIGINT           NOT NULL DEFAULT 0,

    -- Core Auction Metrics
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DEFAULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_buy_qty    DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_sell_qty   DOUBLE PRECISION NOT NULL DEFAULT 0,
    change_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Flattened analytics object layer
    analytics        JSONB            NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

-- 1. Convert to Hypertable
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- 2. Native Compression Policy (Compress older than 7 days)
ALTER TABLE gidh_bars SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_token, timeframe',
    timescaledb.compress_orderby = 'timestamp DESC'
    );

SELECT add_compression_policy('gidh_bars', INTERVAL '7 days', if_not_exists => TRUE);

-- 3. Native Data Retention Policy (Drop older than 45 days)
SELECT add_retention_policy('gidh_bars', INTERVAL '45 days', if_not_exists => TRUE);

-- 4. Compound Indexing
CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_analytics_gin
    ON gidh_bars USING gin (analytics);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_jsonb_peak_vol
    ON gidh_bars (((analytics->>'volume_rank')::integer), timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_jsonb_absorption_seek
    ON gidh_bars (((analytics->>'volume_rank')::integer), ((analytics->>'price_rank')::integer), timestamp DESC);