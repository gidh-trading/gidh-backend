-- schema/007_create_trade_tables.sql

CREATE TABLE IF NOT EXISTS gidh_orders
(
    trading_date DATE        NOT NULL DEFAULT CURRENT_DATE,
    order_id     TEXT PRIMARY KEY,
    symbol       TEXT        NOT NULL,
    product      TEXT        NOT NULL,
    side         TEXT        NOT NULL, -- BUY/SELL
    order_type   TEXT        NOT NULL, -- MARKET/LIMIT
    quantity     INTEGER     NOT NULL,
    filled_qty   INTEGER              DEFAULT 0,
    price        DOUBLE PRECISION,
    status       TEXT        NOT NULL, -- PENDING, COMPLETE, CANCELLED
    timestamp    TIMESTAMPTZ NOT NULL,
    target_price DOUBLE PRECISION,
    sl_price     DOUBLE PRECISION,
    user_email   TEXT,
    sibling_order_id TEXT
);

CREATE TABLE IF NOT EXISTS gidh_positions
(
    trading_date    DATE             NOT NULL,
    symbol          TEXT             NOT NULL,
    product         TEXT             NOT NULL,
    side            TEXT, -- LONG/SHORT
    net_quantity    INTEGER          NOT NULL,
    avg_price       DOUBLE PRECISION NOT NULL,
    realized_pnl    DOUBLE PRECISION DEFAULT 0,
    target_price    DOUBLE PRECISION DEFAULT 0,
    stop_loss_price DOUBLE PRECISION DEFAULT 0,
    updated_at      TIMESTAMPTZ      DEFAULT NOW(),
    PRIMARY KEY (trading_date, symbol, product)
);