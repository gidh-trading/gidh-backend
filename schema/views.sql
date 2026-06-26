DROP VIEW IF EXISTS v_gidh_bars_1m_analytics;
CREATE OR REPLACE VIEW v_gidh_bars_1m_analytics AS
SELECT timestamp,
       instrument_token,
       stock_name,
       timeframe,
       open,
       high,
       low,
       close,
       volume,
       tick_count,
       vwap,
       poc,
       vah,
       val,
       total_buy_qty,
       total_sell_qty,
       change_pct,

       -- Flattened Structural Rank Blends
       (analytics ->> 'volume_rank')::INTEGER                 AS volume_rank,
       (analytics ->> 'tick_rank')::INTEGER                   AS tick_rank,
       (analytics ->> 'price_rank')::INTEGER                  AS price_rank,
       (analytics ->> 'range_rank')::INTEGER                  AS range_rank,
       (analytics ->> 'direction')                            AS direction,

       -- Running Continuous Accumulators
       (analytics ->> 'continuous_volume_intensity')::NUMERIC AS continuous_volume_intensity,
       (analytics ->> 'continuous_price_normalized')::NUMERIC AS continuous_price_normalized,

       -- Retained Helper Distances
       (analytics ->> 'normalized_vwap_distance')::NUMERIC    AS normalized_vwap_distance,
       (analytics ->> 'vwap_close_pct')::NUMERIC              AS vwap_close_pct

FROM gidh_bars
WHERE timeframe = '1m'
  and stock_name != 'NIFTY50'
  AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time >= '09:15:00'
  AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time <= '15:00:00';

DROP VIEW IF EXISTS v_gidh_bars_5m_analytics;
CREATE OR REPLACE VIEW v_gidh_bars_5m_analytics AS
SELECT timestamp,
       instrument_token,
       stock_name,
       timeframe,
       open,
       high,
       low,
       close,
       volume,
       tick_count,
       vwap,
       poc,
       vah,
       val,
       total_buy_qty,
       total_sell_qty,
       change_pct,

       -- Flattened Structural Rank Blends
       (analytics ->> 'volume_rank')::INTEGER                 AS volume_rank,
       (analytics ->> 'tick_rank')::INTEGER                   AS tick_rank,
       (analytics ->> 'price_rank')::INTEGER                  AS price_rank,
       (analytics ->> 'range_rank')::INTEGER                  AS range_rank,
       (analytics ->> 'direction')                            AS direction,

       -- Running Continuous Accumulators
       (analytics ->> 'continuous_volume_intensity')::NUMERIC AS continuous_volume_intensity,
       (analytics ->> 'continuous_price_normalized')::NUMERIC AS continuous_price_normalized,

       -- Retained Helper Distances
       (analytics ->> 'normalized_vwap_distance')::NUMERIC    AS normalized_vwap_distance,
       (analytics ->> 'vwap_close_pct')::NUMERIC              AS vwap_close_pct

FROM gidh_bars
WHERE timeframe = '5m'
  and stock_name != 'NIFTY50'
  AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time >= '09:15:00'
  AND (timestamp AT TIME ZONE 'Asia/Kolkata')::time <= '15:00:00';


-- 1. Create the materialized view tracking distinct dates
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_live_tick_dates
    WITH (timescaledb.continuous) AS
SELECT time_bucket('1 day', timestamp) AS trading_date,
       COUNT(*)                        as tick_count -- Timescale requires an aggregate function for continuous views
FROM live_ticks
GROUP BY time_bucket('1 day', timestamp);



SELECT remove_continuous_aggregate_policy('mv_live_tick_dates', if_exists => TRUE);

-- 2. Add a refresh policy to run every 12 hours
SELECT add_continuous_aggregate_policy('mv_live_tick_dates',
                                       start_offset => INTERVAL '45 days',
                                       end_offset => INTERVAL '12 hours',
                                       schedule_interval => INTERVAL '12 hours'); -- Changed from '1 hour' to '12 hours'
