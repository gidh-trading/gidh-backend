CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Create live_ticks table
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

-- Convert to hypertable (time-series optimization)
SELECT create_hypertable('live_ticks', 'timestamp', if_not_exists => TRUE);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_live_ticks_token ON live_ticks (instrument_token, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_live_ticks_stock ON live_ticks (stock_name, timestamp DESC);
