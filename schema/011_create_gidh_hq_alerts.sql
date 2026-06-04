-- schema/011_create_gidh_hq_alerts.sql
CREATE TABLE IF NOT EXISTS gidh_hq_alerts
(
    alert_id         UUID DEFAULT gen_random_uuid(),
    timestamp        TIMESTAMPTZ      NOT NULL,
    instrument_token INT              NOT NULL,
    stock_name       VARCHAR(50)      NOT NULL,
    alert_type       VARCHAR(30)      NOT NULL, -- 'PLAYABLE_BREAKOUT', 'ABSORPTION_TRAP', 'STRATEGIC_EXIT'
    direction        VARCHAR(20)      NOT NULL, -- 'BULLISH', 'BEARISH'
    vwp_delta        DOUBLE PRECISION NOT NULL, -- Evaluated capital mass
    efficiency       DOUBLE PRECISION NOT NULL, -- Execution friction score
    price            DOUBLE PRECISION NOT NULL, -- Execution mark coordinate
    msg              TEXT             NOT NULL,
    PRIMARY KEY (timestamp, alert_id)
);
SELECT create_hypertable('gidh_hq_alerts', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_hq_alerts_token_time ON gidh_hq_alerts (instrument_token, timestamp DESC);