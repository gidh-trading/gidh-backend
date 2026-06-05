-- schema/006_create_gidh_bars.sql
DROP TABLE IF EXISTS gidh_bars CASCADE;

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
    tick_count       BIGINT           NOT NULL DEFAULT 0, -- Upgraded to BIGINT to safeguard heavy multi-hour tick windows

    -- Core Auction Metrics
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DEFAULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_buy_qty    DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_sell_qty   DOUBLE PRECISION NOT NULL DEFAULT 0,
    change_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- 🔥 Flattened analytics object layer containing volume_rank, tick_rank, price_rank, range_rank, and direction
    analytics        JSONB            NOT NULL DEFAULT '{}'::jsonb,

    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

-- Convert to a TimescaleDB hypertable for optimized time-series chunking
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- Primary compound indexing configurations for timeframe retrieval patterns
CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);

-- 🔥 Performance GIN Index: High-speed native lookup across any internal key within your analytics parameters
CREATE INDEX IF NOT EXISTS idx_gidh_bars_analytics_gin
    ON gidh_bars USING gin (analytics);

-- 🔥 Performance Expression Index: Instant filtering for bars with P90+ Volume Bursts nested inside the JSONB structure
CREATE INDEX IF NOT EXISTS idx_gidh_bars_jsonb_peak_vol
    ON gidh_bars (((analytics->>'volume_rank')::integer), timestamp DESC);

-- 🔥 Performance Expression Index: Instant filtering for compressed or institutional absorption tracking states
CREATE INDEX IF NOT EXISTS idx_gidh_bars_jsonb_absorption_seek
    ON gidh_bars (((analytics->>'volume_rank')::integer), ((analytics->>'price_rank')::integer), timestamp DESC);