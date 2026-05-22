-- Create order_depth table
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

-- Convert to hypertable (time-series optimization)
SELECT create_hypertable('live_order_depth', 'timestamp', if_not_exists => TRUE);

-- Create indexes for efficient querying
CREATE INDEX IF NOT EXISTS idx_order_depth_token ON live_order_depth (instrument_token, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_depth_stock ON live_order_depth (stock_name, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_order_depth_side ON live_order_depth (side);
CREATE INDEX IF NOT EXISTS idx_order_depth_token_side ON live_order_depth (instrument_token, side, timestamp DESC);

SELECT add_retention_policy('live_order_depth', INTERVAL '14 days', if_not_exists => TRUE);
