-- Schema 1: High-Fidelity Scatter Plot Grid Matrix Coordinates
CREATE TABLE gidh_anomaly_grids
(
    time_bin         TIMESTAMPTZ    NOT NULL,
    instrument_token INT            NOT NULL,
    price_bin        NUMERIC(12, 4) NOT NULL,
    buy_volume       BIGINT         NOT NULL,
    sell_volume      BIGINT         NOT NULL,
    total_volume     BIGINT         NOT NULL,
    peak_z_score     REAL           NOT NULL,
    tick_count       INT            NOT NULL,
    cluster_vwap     NUMERIC(12, 2) NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    -- Primary key combines space and time to eliminate indexing duplication faults
    PRIMARY KEY (time_bin, instrument_token, price_bin)
);

-- Convert standard structural DDL table layouts into a partitioned TimescaleDB Hypertable
SELECT create_hypertable('gidh_anomaly_grids', 'time_bin');

-- Build hyper-targeted covered index trees for high-speed REST API chart initialization queries
CREATE INDEX idx_anomaly_grids_query
    ON gidh_anomaly_grids (instrument_token, time_bin DESC, price_bin);


-- ---------------------------------------------------------------------------------------


-- Schema 2: Instant Whale Block Alert Transaction Logs (Zero Latency Records)
CREATE TABLE gidh_whale_blocks
(
    timestamp        TIMESTAMPTZ    NOT NULL,
    instrument_token INT            NOT NULL,
    price            NUMERIC(12, 2) NOT NULL,
    volume           BIGINT         NOT NULL,
    side             VARCHAR(8)     NOT NULL, -- 'BUY' or 'SELL' via Lee-Ready
    v_expected       REAL           NOT NULL, -- Contextual expected baseline profile marker
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Partition the alert logger tracking table by time
SELECT create_hypertable('gidh_whale_blocks', 'timestamp');

-- Index for real-time order tracking dashboard panels
CREATE INDEX idx_whale_blocks_query
    ON gidh_whale_blocks (instrument_token, timestamp DESC);