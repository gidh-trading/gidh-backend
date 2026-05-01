-- schema/005_create_market_dna.sql
CREATE TABLE IF NOT EXISTS gidh_market_dna
(
    instrument_token INTEGER,
    stock_name       TEXT NOT NULL,
    trading_date     DATE NOT NULL,
    poc_5d           DOUBLE PRECISION,
    vah_5d           DOUBLE PRECISION,
    val_5d           DOUBLE PRECISION,
    macro_hvns       JSONB,
    macro_lvns       JSONB,
    time_buckets     JSONB,
    updated_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (instrument_token, trading_date) -- Required for ON CONFLICT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_dna_date ON gidh_market_dna (trading_date DESC);