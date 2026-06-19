CREATE OR REPLACE VIEW gidh_bars_1m AS
SELECT *
FROM gidh_bars
WHERE (EXTRACT(HOUR FROM "timestamp") > 9
    OR (EXTRACT(HOUR FROM "timestamp") = 9 AND EXTRACT(MINUTE FROM "timestamp") > 19))
  AND (EXTRACT(HOUR FROM "timestamp") < 10
    OR (EXTRACT(HOUR FROM "timestamp") = 10 AND EXTRACT(MINUTE FROM "timestamp") < 20));