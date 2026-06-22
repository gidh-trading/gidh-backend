CREATE OR REPLACE VIEW view_stock_daily_extreme_vwap_percentiles AS
WITH daily_extremes AS (
    SELECT
        DATE(timestamp) AS trading_day,
        instrument_token,
        stock_name,
        timeframe,
        MAX((analytics->>'normalized_vwap_distance')::DOUBLE PRECISION) AS daily_max_positive,
        -- Take the ABSOLUTE value of the negative stretch so it sorts intuitively (e.g., -0.50 becomes 0.50)
        ABS(MIN((analytics->>'normalized_vwap_distance')::DOUBLE PRECISION)) AS daily_max_negative_abs
    FROM gidh_bars
    GROUP BY DATE(timestamp), instrument_token, stock_name, timeframe
)
SELECT
    instrument_token,
    stock_name,
    timeframe,

    -- --- Upper Extreme Percentiles (Unchanged) ---
    percentile_cont(0.05) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p05,
    percentile_cont(0.10) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p10,
    percentile_cont(0.25) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p25,
    percentile_cont(0.50) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p50_median,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p75,
    percentile_cont(0.90) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p90,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY daily_max_positive) AS upper_p95,

    -- --- Lower Extreme Percentiles (Flipped to negative at the end) ---
    -- Now P95 represents the biggest downward spikes (-0.50) instead of the smallest ones
    -percentile_cont(0.05) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p05,
    -percentile_cont(0.10) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p10,
    -percentile_cont(0.25) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p25,
    -percentile_cont(0.50) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p50_median,
    -percentile_cont(0.75) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p75,
    -percentile_cont(0.90) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p90,
    -percentile_cont(0.95) WITHIN GROUP (ORDER BY daily_max_negative_abs) AS lower_p95,

    COUNT(trading_day) AS total_days_analyzed
FROM daily_extremes
where stock_name != 'NIFTY50'
GROUP BY instrument_token, stock_name, timeframe;