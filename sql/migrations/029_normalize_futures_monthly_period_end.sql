-- Migration 029: normalize futures monthly K-line keys to calendar month-end

WITH ranked AS (
    SELECT
        symbol,
        time,
        ROW_NUMBER() OVER (
            PARTITION BY symbol, date_trunc('month', time AT TIME ZONE 'Asia/Shanghai')
            ORDER BY
                (time AT TIME ZONE 'Asia/Shanghai')::date =
                    (date_trunc('month', time AT TIME ZONE 'Asia/Shanghai')
                     + INTERVAL '1 month - 1 day')::date DESC,
                updated_at DESC NULLS LAST,
                time DESC
        ) AS row_number
    FROM futures.monthly
)
DELETE FROM futures.monthly monthly
USING ranked
WHERE monthly.symbol = ranked.symbol
  AND monthly.time = ranked.time
  AND ranked.row_number > 1;

UPDATE futures.monthly
SET time = (
        date_trunc('month', time AT TIME ZONE 'Asia/Shanghai')
        + INTERVAL '1 month - 1 day 15 hours'
    ) AT TIME ZONE 'Asia/Shanghai',
    updated_at = NOW()
WHERE (time AT TIME ZONE 'Asia/Shanghai')::date <>
      (date_trunc('month', time AT TIME ZONE 'Asia/Shanghai')
       + INTERVAL '1 month - 1 day')::date;
