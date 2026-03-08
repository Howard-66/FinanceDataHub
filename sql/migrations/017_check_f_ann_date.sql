-- 检查源表 f_ann_date_time 的数据完整性

-- 1. 检查各表 f_ann_date_time 的小时分布
SELECT 'cashflow' as tbl, EXTRACT(HOUR FROM f_ann_date_time) as hr, COUNT(*) as cnt
FROM cashflow GROUP BY EXTRACT(HOUR FROM f_ann_date_time)
UNION ALL
SELECT 'income', EXTRACT(HOUR FROM f_ann_date_time), COUNT(*)
FROM income GROUP BY EXTRACT(HOUR FROM f_ann_date_time)
UNION ALL
SELECT 'balancesheet', EXTRACT(HOUR FROM f_ann_date_time), COUNT(*)
FROM balancesheet GROUP BY EXTRACT(HOUR FROM f_ann_date_time)
UNION ALL
SELECT 'fina_indicator', EXTRACT(HOUR FROM ann_date_time), COUNT(*)
FROM fina_indicator GROUP BY EXTRACT(HOUR FROM ann_date_time)
ORDER BY tbl, hr;

-- 5. 检查 processed_fundamental_quality 中 f_ann_date_time 为 NULL 的样本
SELECT p.ts_code, p.end_date_time,
       c.f_ann_date_time as cf_f_ann,
       i.f_ann_date_time as inc_f_ann,
       b.f_ann_date_time as bs_f_ann,
       f.ann_date_time as fina_ann
FROM processed_fundamental_quality p
LEFT JOIN cashflow c ON p.ts_code = c.ts_code AND p.end_date_time = c.end_date_time
LEFT JOIN income i ON p.ts_code = i.ts_code AND p.end_date_time = i.end_date_time
LEFT JOIN balancesheet b ON p.ts_code = b.ts_code AND p.end_date_time = b.end_date_time
LEFT JOIN fina_indicator f ON p.ts_code = f.ts_code AND p.end_date_time = f.end_date_time
WHERE p.f_ann_date_time IS NULL
LIMIT 10;