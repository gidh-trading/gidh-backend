CREATE TABLE IF NOT EXISTS gidh_bars
(
    timestamp        TIMESTAMPTZ      NOT NULL,
    instrument_token INTEGER          NOT NULL,
    stock_name       TEXT             NOT NULL,
    timeframe        TEXT             NOT NULL, -- '1m' or '5m'

    -- OHLCV Data
    open             DOUBLE PRECISION NOT NULL,
    high             DOUBLE PRECISION NOT NULL,
    low              DOUBLE PRECISION NOT NULL,
    close            DOUBLE PRECISION NOT NULL,
    volume           DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Value Area / Profile Stats
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_volume       DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_volume      DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Energy Metrics (The 6 Fact Columns)
    total_vol_energy DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_vol_energy   DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_vol_energy  DOUBLE PRECISION NOT NULL DEFAULT 0,

    total_rng_energy DOUBLE PRECISION NOT NULL DEFAULT 0,
    buy_rng_energy   DOUBLE PRECISION NOT NULL DEFAULT 0,
    sell_rng_energy  DOUBLE PRECISION NOT NULL DEFAULT 0,

    -- Primary Key must include the partitioning column (timestamp) in TimescaleDB.
    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

-- 1. Convert to hypertable partitioned by 'timestamp'.
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- 2. Create index for the UI to fetch recent history for a specific stock efficiently.
CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

-- 3. Optional: Index specifically for timeframe filtering if you query 1m and 5m separately.
CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);