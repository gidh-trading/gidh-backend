-- schema/008_create_volume_regime_sessions.sql

CREATE TABLE IF NOT EXISTS gidh_volume_regime_sessions
(
    timestamp          TIMESTAMPTZ      NOT NULL, -- Mandatory partitioning column for snapshot frame generation
    instrument_token   INTEGER          NOT NULL,
    stock_name         TEXT             NOT NULL,
    minute_index       INTEGER          NOT NULL, -- Current/Ending time-bucket identifier for dynamic DNA ranking

    -- Regime State Flags
    active             BOOLEAN          NOT NULL, -- true = Ongoing live tracking, false = Concluded/Terminated session record
    anomaly_type       TEXT             NOT NULL, -- "BURST" or "ABSORPTION"
    direction          INTEGER          NOT NULL, -- 1 = BULLISH, -1 = BEARISH, 0 = FLAT

    -- Operational Lifespan Boundaries
    start_time         TIMESTAMPTZ      NOT NULL, -- Exact tick timestamp when participation crossed >= P90
    end_time           TIMESTAMPTZ      NOT NULL, -- Exact tick timestamp when participation collapsed back below threshold

    -- Core Execution Metrics
    start_price        DOUBLE PRECISION NOT NULL,
    current_price      DOUBLE PRECISION NOT NULL, -- Serves as the final "end_price" once active = false
    signed_move        DOUBLE PRECISION NOT NULL, -- CurrentPrice - StartPrice
    abs_move           DOUBLE PRECISION NOT NULL, -- Absolute displacement magnitude

    -- Multi-Minute Percentile Ranks
    peak_volume_rank   INTEGER          NOT NULL, -- Highest recorded linear volume percentile score (6-7) achieved
    current_price_rank INTEGER          NOT NULL, -- Price rank evaluated against the anomaly ending minute index DNA

    PRIMARY KEY (timestamp, instrument_token)     -- Enforced TimescaleDB hypertable constraint layout
);

-- Convert to a TimescaleDB hypertable for optimized partitioning and chunking by the time dimension
SELECT create_hypertable('gidh_volume_regime_sessions', 'timestamp', if_not_exists => TRUE);

-- Compound indexing configurations optimized for fast historical watchtower instrument analysis queries
CREATE INDEX IF NOT EXISTS idx_regime_sessions_token_time
    ON gidh_volume_regime_sessions (instrument_token, timestamp DESC);

-- Automatic retention strategy matching your raw data tracking engine parameters
SELECT add_retention_policy('gidh_volume_regime_sessions', INTERVAL '30 days', if_not_exists => TRUE);