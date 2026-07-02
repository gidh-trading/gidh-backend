DROP TABLE IF EXISTS gidh_vwap_distance_percentiles;

CREATE TABLE gidh_vwap_distance_percentiles
(
    instrument_token INTEGER      NOT NULL,
    stock_name       VARCHAR(255) NOT NULL,
    trading_date     DATE         NOT NULL,
    -- Positive Extensions Pool (Price >= VWAP)
    pos_p50          FLOAT        NOT NULL,
    pos_p75          FLOAT        NOT NULL,
    pos_p90          FLOAT        NOT NULL,
    pos_p97          FLOAT        NOT NULL,
    pos_p99          FLOAT        NOT NULL, -- Added p99
    -- Negative Extensions Pool (Price < VWAP, stored as absolute magnitude)
    neg_p50          FLOAT        NOT NULL,
    neg_p75          FLOAT        NOT NULL,
    neg_p90          FLOAT        NOT NULL,
    neg_p97          FLOAT        NOT NULL,
    neg_p99          FLOAT        NOT NULL, -- Added p99
    updated_at       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    PRIMARY KEY (instrument_token, trading_date)
);

ALTER TABLE gidh_vwap_distance_percentiles
    ADD COLUMN pos_max DOUBLE PRECISION DEFAULT 0.0,
    ADD COLUMN neg_max DOUBLE PRECISION DEFAULT 0.0;

ALTER TABLE gidh_vwap_distance_percentiles
    ADD COLUMN max_pos_change_pct NUMERIC,
    ADD COLUMN max_neg_change_pct NUMERIC;

CREATE INDEX idx_gidh_vwap_dist_date ON gidh_vwap_distance_percentiles (trading_date);