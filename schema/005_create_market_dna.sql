CREATE TABLE IF NOT EXISTS gidh_market_dna
(
    instrument_token     INTEGER NOT NULL,
    stock_name           TEXT    NOT NULL,
    trading_date         DATE    NOT NULL,
    poc_5d               DOUBLE PRECISION,
    vah_5d               DOUBLE PRECISION,
    val_5d               DOUBLE PRECISION,
    macro_hvns           JSONB       DEFAULT '{}'::jsonb,
    macro_lvns           JSONB       DEFAULT '{}'::jsonb,
    time_buckets         JSONB       DEFAULT '{}'::jsonb,
    interval_percentiles JSONB       DEFAULT '{}'::jsonb,
    updated_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (instrument_token, trading_date)
);

-- Convert to Hypertable
SELECT create_hypertable('gidh_market_dna', 'trading_date',
                         if_not_exists => TRUE,
                         migrate_data => TRUE);

-- Performance Index
CREATE INDEX IF NOT EXISTS idx_dna_token_date
    ON gidh_market_dna (instrument_token, trading_date DESC);

-- Automated 30-Day Cleanup
SELECT add_retention_policy('gidh_market_dna', INTERVAL '45 days', if_not_exists => TRUE);