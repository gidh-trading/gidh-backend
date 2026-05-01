-- migrations/003_create_instrument_configs_table.sql
CREATE TABLE IF NOT EXISTS instrument_configs
(
    instrument_token INTEGER PRIMARY KEY,
    stock_name       TEXT             NOT NULL,
    symbol           TEXT             NOT NULL,
    bucket_size      DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ib_duration_mins INTEGER          NOT NULL DEFAULT 60,
    avg_daily_volume BIGINT           NOT NULL DEFAULT 0,
    max_sl_dist      DOUBLE PRECISION NOT NULL DEFAULT 10.0,
    is_active        BOOLEAN                   DEFAULT TRUE,
    is_backtest        BOOLEAN                   DEFAULT FALSE,
    updated_at       TIMESTAMPTZ               DEFAULT NOW()
);


insert into public.instrument_configs (instrument_token, stock_name, symbol, bucket_size, ib_duration_mins, avg_daily_volume, max_sl_dist, is_active, is_backtest)
values  (2995969, 'ALKEM', 'ALKEM', 2, 60, 326997, 10, true, false),
        (2941697, 'APARINDS', 'APARINDS', 2, 60, 128110, 10, true, false),
        (4267265, 'BAJAJ-AUTO', 'BAJAJ-AUTO', 2, 60, 364575, 10, true, false),
        (140033, 'BRITANNIA', 'BRITANNIA', 2, 60, 480033, 10, true, false),
        (3905025, 'CEATLTD', 'CEATLTD', 1, 60, 94134, 10, true, false),
        (2800641, 'DIVISLAB', 'DIVISLAB', 2, 60, 289194, 10, true, false),
        (4296449, 'GVT&D', 'GVT&D', 1, 60, 602523, 10, true, false),
        (589569, 'HAL', 'HAL', 1, 60, 1101937, 10, true, false),
        (345089, 'HEROMOTOCO', 'HEROMOTOCO', 2, 60, 1064396, 10, true, false),
        (3095553, 'KAYNES', 'KAYNES', 1, 60, 1243630, 10, true, false),
        (3407361, 'KEI', 'KEI', 1, 60, 248695, 10, true, false),
        (4561409, 'LTM', 'LTM', 1, 60, 1133287, 10, true, false),
        (693505, 'MTARTECH', 'MTARTECH', 1, 60, 194181, 10, true, false),
        (3756033, 'NAVINFLUOR', 'NAVINFLUOR', 2, 60, 147359, 10, true, false),
        (2455041, 'POLYCAB', 'POLYCAB', 2, 60, 462869, 10, true, false),
        (860929, 'SUPREMEIND', 'SUPREMEIND', 1, 60, 194331, 10, true, false),
        (873217, 'TATAELXSI', 'TATAELXSI', 1, 60, 860165, 10, true, false),
        (4638209, 'THANGAMAYL', 'THANGAMAYL', 1, 60, 104249, 10, true, false),
        (900609, 'TORNTPHARM', 'TORNTPHARM', 1, 60, 271332, 10, true, false),
        (2952193, 'ULTRACEMCO', 'ULTRACEMCO', 5, 60, 377472, 10, true, false),
        (486657, 'CUMMINSIND', 'CUMMINSIND', 1, 60, 889569, 10, true, false),
        (502785, 'TRENT', 'TRENT', 1, 60, 856691, 10, true, false),
        (4701441, 'PERSISTENT', 'PERSISTENT', 1, 60, 665831, 10, true, false),
        (1883649, 'DATAPATTNS', 'DATAPATTNS', 1, 60, 607520, 10, true, false),
        (232961, 'EICHERMOT', 'EICHERMOT', 2, 60, 587696, 10, true, false),
        (258817, 'SCHAEFFLER', 'SCHAEFFLER', 1, 60, 258038, 10, true, false),
        (889601, 'THERMAX', 'THERMAX', 1, 60, 236969, 10, true, false),
        (5552641, 'DIXON', 'DIXON', 5, 60, 574357, 10, true, true),
        (303361, 'AMBER', 'AMBER', 2, 60, 183162, 10, true, true),
        (2815745, 'MARUTI', 'MARUTI', 5, 60, 745873, 10, true, true),
        (40193, 'APOLLOHOSP', 'APOLLOHOSP', 2, 60, 707640, 10, true, true),
        (897537, 'TITAN', 'TITAN', 1, 60, 1112789, 10, true, true);