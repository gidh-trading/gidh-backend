-- schema/010_create_gidh_hq_stock_states.sql
CREATE TABLE IF NOT EXISTS gidh_hq_stock_states
(
    timestamp          TIMESTAMPTZ      NOT NULL,
    instrument_token   INT              NOT NULL,
    stock_name         VARCHAR(50)      NOT NULL,
    vwp_delta          DOUBLE PRECISION NOT NULL,
    auction_efficiency DOUBLE PRECISION NOT NULL,
    detected_direction VARCHAR(20)      NOT NULL, -- 'BULLISH', 'BEARISH', 'NONE'
    is_absorbing       BOOLEAN          NOT NULL,
    PRIMARY KEY (timestamp, instrument_token)
);
SELECT create_hypertable('gidh_hq_stock_states', 'timestamp', if_not_exists => TRUE);