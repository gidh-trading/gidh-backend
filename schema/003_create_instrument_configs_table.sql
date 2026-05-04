create table public.instrument_configs
(
    instrument_token integer               not null
        primary key,
    stock_name       text                  not null,
    is_backtest      boolean default false not null
);

create table public.instrument_profile
(
    instrument_token bigint not null
        primary key
        constraint fk_token
            references public.instrument_configs
            on delete cascade,
    bucket_size      numeric(10, 2),
    atr_14           numeric(10, 2),
    adr_pct          numeric(5, 2),
    adv_30d          bigint,
    adv_val_30d      numeric(15, 2),
    updated_at       timestamp default CURRENT_TIMESTAMP
);


insert into public.instrument_configs (instrument_token, stock_name, is_backtest)
values  (232961, 'EICHERMOT', false),
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
        (303361, 'AMBER', true);
