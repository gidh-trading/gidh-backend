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

    -- Dynamic Anomaly Metadata Document Storage
    metrics          JSONB            NOT NULL,

    -- Core Auction Metrics
    vwap             DOUBLE PRECISION NOT NULL DEFAULT 0,
    poc              DOUBLE PRECISION NOT NULL DEFAULT 0,
    vah              DOUBLE PRECISION NOT NULL DEFAULT 0,
    val              DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_buy_qty    INTEGER          NOT NULL DEFAULT 0,
    total_sell_qty   INTEGER          NOT NULL DEFAULT 0,
    change_pct       DOUBLE PRECISION NOT NULL DEFAULT 0,

    PRIMARY KEY (timestamp, instrument_token, timeframe)
);

SELECT create_hypertable('gidh_bars', 'timestamp', if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_token_time
    ON gidh_bars (instrument_token, timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_gidh_bars_timeframe
    ON gidh_bars (timeframe, timestamp DESC);