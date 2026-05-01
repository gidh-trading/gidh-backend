-- schema/004_create_live_volume_profiles.sql
CREATE TABLE IF NOT EXISTS gidh_volume_profiles
(
    instrument_token INTEGER NOT NULL,
    stock_name       TEXT NOT NULL,
    trading_date     DATE NOT NULL,

    total_volume     BIGINT DEFAULT 0,

    -- Tactical Levels
    poc              DOUBLE PRECISION,
    vah              DOUBLE PRECISION,
    val              DOUBLE PRECISION,

    -- JSON Arrays for UI and downstream analysis
    nodes            JSONB,
    hvns             JSONB,
    lvns             JSONB,

    updated_at       TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (instrument_token, trading_date)
);

-- Index to quickly load today's profiles on a backend restart
CREATE INDEX IF NOT EXISTS idx_vp_trading_date ON gidh_volume_profiles (trading_date DESC);