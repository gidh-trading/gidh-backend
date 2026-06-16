-- Up
CREATE TABLE IF NOT EXISTS strategy_transactions
(
    id              BIGSERIAL PRIMARY KEY,
    trade_id        VARCHAR(64)    NOT NULL,            -- Links an entry and its corresponding exit row together
    strategy_name   VARCHAR(100)   NOT NULL,
    instrument      VARCHAR(50)    NOT NULL,
    action_type     VARCHAR(20)    NOT NULL,            -- 'GO_LONG', 'GO_SHORT', 'EXIT_LONG', 'EXIT_SHORT'
    price           NUMERIC(18, 4) NOT NULL,
    quantity        NUMERIC(18, 4) NOT NULL,
    execution_time  TIMESTAMPTZ    NOT NULL,
    trigger_reason  VARCHAR(255)   NOT NULL,            -- e.g., "VWAP_Cross_Above", "Stop_Loss", "Take_Profit"

    -- Performance Metrics captured exactly at this point in time
    current_pnl     NUMERIC(18, 4) DEFAULT 0.0000,
    peak_pnl        NUMERIC(18, 4) DEFAULT 0.0000,      -- Tracks max run-up observed during this lifecycle phase

    -- Extensible context for forensic strategy metrics
    market_snapshot JSONB          DEFAULT '{}'::jsonb, -- Captures indicator arrays or market state weights
    created_at      TIMESTAMPTZ    DEFAULT CURRENT_TIMESTAMP
);

-- Fast analytical indices
CREATE INDEX IF NOT EXISTS idx_strat_tx_trade_id ON strategy_transactions (trade_id);
CREATE INDEX IF NOT EXISTS idx_strat_tx_strat_instrument ON strategy_transactions (strategy_name, instrument);
CREATE INDEX IF NOT EXISTS idx_strat_tx_execution_time ON strategy_transactions (execution_time DESC);