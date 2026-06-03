DROP TABLE IF EXISTS public.gidh_bars_price_potential CASCADE;

CREATE TABLE public.gidh_bars_price_potential
(
    stock_name TEXT             NOT NULL,
    timeframe  TEXT             NOT NULL, -- 🎯 Replaced 'interval' with 'timeframe' to fix SQL keyword conflict
    p97        DOUBLE PRECISION NOT NULL DEFAULT 0,
    p90        DOUBLE PRECISION NOT NULL DEFAULT 0,
    p75        DOUBLE PRECISION NOT NULL DEFAULT 0,
    p50        DOUBLE PRECISION NOT NULL DEFAULT 0,
    p25        DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (stock_name, timeframe)
);

-- Index for instant lookup on parameters passed from the API
CREATE INDEX idx_price_potential_lookup
    ON public.gidh_bars_price_potential (stock_name, timeframe);

CREATE OR REPLACE PROCEDURE public.refresh_gidh_bars_price_potential()
    LANGUAGE plpgsql
AS
$$
BEGIN
    RAISE NOTICE 'Starting calculation of 15-day price potential medians...';

    INSERT INTO public.gidh_bars_price_potential (stock_name, timeframe, p97, p90, p75, p50, p25, updated_at)
    WITH last_15_days_data AS (SELECT stock_name,
                                      timeframe,
                                      CASE price_rank
                                          WHEN 7 THEN 'p97'
                                          WHEN 6 THEN 'p90'
                                          WHEN 5 THEN 'p75'
                                          WHEN 4 THEN 'p50'
                                          ELSE 'p25'
                                          END           AS price_percentile_tier,
                                      ABS(close - open) AS body_movement
                               FROM public.gidh_bars
                               WHERE "timestamp" >= NOW() - INTERVAL '15 days'),
         median_calculations AS (SELECT stock_name,
                                        timeframe,
                                        price_percentile_tier,
                                        percentile_cont(0.5) WITHIN GROUP (ORDER BY body_movement) AS median_body
                                 FROM last_15_days_data
                                 GROUP BY stock_name, timeframe, price_percentile_tier)
    SELECT stock_name,
           timeframe,
           ROUND(COALESCE(MAX(CASE WHEN price_percentile_tier = 'p97' THEN median_body END)::numeric, 0), 1) AS p97,
           ROUND(COALESCE(MAX(CASE WHEN price_percentile_tier = 'p90' THEN median_body END)::numeric, 0), 1) AS p90,
           ROUND(COALESCE(MAX(CASE WHEN price_percentile_tier = 'p75' THEN median_body END)::numeric, 0), 1) AS p75,
           ROUND(COALESCE(MAX(CASE WHEN price_percentile_tier = 'p50' THEN median_body END)::numeric, 0), 1) AS p50,
           ROUND(COALESCE(MAX(CASE WHEN price_percentile_tier = 'p25' THEN median_body END)::numeric, 0), 1) AS p25,
           NOW()                                                                                             AS updated_at
    FROM median_calculations
    GROUP BY stock_name, timeframe
    ON CONFLICT (stock_name, timeframe)
        DO UPDATE SET p97        = EXCLUDED.p97,
                      p90        = EXCLUDED.p90,
                      p75        = EXCLUDED.p75,
                      p50        = EXCLUDED.p50,
                      p25        = EXCLUDED.p25,
                      updated_at = EXCLUDED.updated_at;

    RAISE NOTICE 'Price potential table refreshed successfully.';
END;
$$;

CALL public.refresh_gidh_bars_price_potential();