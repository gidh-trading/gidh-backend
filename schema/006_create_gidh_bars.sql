CREATE TABLE IF NOT EXISTS gidh_bars
(
    timestamp        TIMESTAMPTZ      NOT NULL,
    instrument_token INTEGER          NOT NULL,
    stock_name       TEXT             NOT NULL,
    timeframe        TEXT             NOT NULL, -- '1m', '3m', or '5m'

    -- OHLCV Data
    open             DOUBLE PRECISION NOT NULL,
    high             DOUBLE PRECISION NOT NULL,
    low              DOUBLE PRECISION NOT NULL,
    close            DOUBLE PRECISION NOT NULL,
    volume           DOUBLE PRECISION NOT NULL DEFAULT 0,
    tick_count       INTEGER          NOT NULL DEFAULT 0,
    max_tick_count_z DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Value Area / Profile Stats
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DEFAULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,

    heatmap          JSONB,

    -- Primary Key must include the partitioning column (timestamp) in TimescaleDB.
    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

-- 1. Convert to hypertable partitioned by 'timestamp'.
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- 2. Create index for the UI to fetch recent history for a specific stock efficiently.
CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

-- 3. Index specifically for timeframe filtering if you query timeframes separately.
CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);