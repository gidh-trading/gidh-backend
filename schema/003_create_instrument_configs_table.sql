create table public.instrument_configs
(
    instrument_token integer               not null primary key,
    stock_name       text                  not null,
    is_backtest      boolean default false not null
);

CREATE TABLE IF NOT EXISTS public.instrument_profile
(
    instrument_token bigint NOT NULL,
    trading_date     DATE NOT NULL,
    bucket_size      NUMERIC(10, 2),
    atr_14           NUMERIC(10, 2),
    adr_pct          NUMERIC(5, 2),
    adv_30d          BIGINT,
    adv_val_30d      NUMERIC(15, 2),
    updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (instrument_token, trading_date),

    CONSTRAINT fk_token
        FOREIGN KEY (instrument_token)
            REFERENCES public.instrument_configs (instrument_token)
            ON DELETE CASCADE
);

-- Convert to Hypertable
SELECT create_hypertable('public.instrument_profile', 'trading_date',
                         if_not_exists => TRUE,
                         migrate_data => TRUE);

-- Automated 30-Day Cleanup
SELECT add_retention_policy('public.instrument_profile', INTERVAL '45 days', if_not_exists => TRUE);


insert into public.instrument_configs (instrument_token, stock_name, is_backtest)
values (232961, 'EICHERMOT', true),
       (2941697, 'APARINDS', false),
       (140033, 'BRITANNIA', false),
       (900609, 'TORNTPHARM', false),
       (2800641, 'DIVISLAB', false),
       (3905025, 'CEATLTD', false),
       (2952193, 'ULTRACEMCO', false),
       (1883649, 'DATAPATTNS', false),
       (2455041, 'POLYCAB', false),
       (5552641, 'DIXON', false),
       (860929, 'SUPREMEIND', false),
       (2995969, 'ALKEM', false),
       (873217, 'TATAELXSI', false),
       (2815745, 'MARUTI', false),
       (486657, 'CUMMINSIND', false),
       (4267265, 'BAJAJ-AUTO', false),
       (258817, 'SCHAEFFLER', false),
       (4701441, 'PERSISTENT', false),
       (3095553, 'KAYNES', false),
       (589569, 'HAL', false),
       (889601, 'THERMAX', false),
       (3407361, 'KEI', false),
       (40193, 'APOLLOHOSP', false),
       (897537, 'TITAN', false),
       (4638209, 'THANGAMAYL', false),
       (3756033, 'NAVINFLUOR', false),
       (693505, 'MTARTECH', false),
       (4561409, 'LTM', false),
       (345089, 'HEROMOTOCO', false),
       (4296449, 'GVT&D', false),
       (502785, 'TRENT', false),
       (303361, 'AMBER', false),
       (256265, 'NIFTY50', false);


CREATE VIEW public.instrument_full_summary AS
SELECT
    c.instrument_token,
    c.stock_name,
    p.trading_date,
    p.bucket_size,
    p.atr_14,
    p.adr_pct,
    p.adv_30d,
    p.adv_val_30d
FROM public.instrument_configs c
         LEFT JOIN public.instrument_profile p
                   ON c.instrument_token = p.instrument_token;