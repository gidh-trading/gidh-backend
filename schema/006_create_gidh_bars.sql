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
    tick_count       INTEGER          NOT NULL DEFAULT 0,

    -- Core Auction Metrics
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DEFAULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_buy_qty    DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_sell_qty   DOUBLE PRECISION NOT NULL DEFAULT 0,
    change_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

-- Convert to a TimescaleDB hypertable for optimized time-series chunking
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- Primary compound indexing configurations for timeframe retrieval patterns
CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);

-- Optional Performance Boost: Functional index for high-speed anomaly filtering
-- This allows you to instantly fetch bars with a specific peak volume rank without parsing the full JSON string inside every row.
CREATE INDEX IF NOT EXISTS idx_gidh_bars_peak_vol
    ON gidh_bars (((peaks->>'peak_volume_rank')::integer), timestamp DESC);