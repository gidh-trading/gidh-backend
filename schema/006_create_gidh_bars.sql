CREATE TABLE IF NOT EXISTS gidh_bars
(
    timestamp        TIMESTAMPTZ      NOT NULL,
    instrument_token INTEGER          NOT NULL,
    stock_name       TEXT,
    timeframe        TEXT             NOT NULL,
    open             DOUBLE PRECISION NOT NULL,
    high             DOUBLE PRECISION NOT NULL,
    low              DOUBLE PRECISION NOT NULL,
    close            DOUBLE PRECISION NOT NULL,
    volume           BIGINT           NOT NULL,
    vwap             DOUBLE PRECISION,
    poc              DOUBLE PRECISION,
    vah              DOUBLE PRECISION,
    val              DOUBLE PRECISION,
    vol_energy       DOUBLE PRECISION,
    rng_energy       DOUBLE PRECISION,

    PRIMARY KEY (timestamp, stock_name, timeframe)
);

-- Convert to hypertable for time-series optimization
-- This partitions the data by the 'time' column
SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

-- Create a composite index for efficient stock_name-based queries
CREATE INDEX IF NOT EXISTS idx_gidh_bars_stock_name_time
    ON gidh_bars (stock_name, timestamp DESC);