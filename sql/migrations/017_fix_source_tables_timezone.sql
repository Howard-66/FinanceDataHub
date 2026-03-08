-- Migration: 017_fix_source_tables_timezone
-- 修复源表 balancesheet 和 income 的 f_ann_date_time 时区问题

-- ============================================
-- 1. 修复 balancesheet 表
-- ============================================
UPDATE balancesheet
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9);

-- 验证
-- SELECT EXTRACT(HOUR FROM f_ann_date_time) as hr, COUNT(*)
-- FROM balancesheet GROUP BY EXTRACT(HOUR FROM f_ann_date_time);

-- ============================================
-- 2. 修复 income 表
-- ============================================
UPDATE income
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9);

-- 验证
-- SELECT EXTRACT(HOUR FROM f_ann_date_time) as hr, COUNT(*)
-- FROM income GROUP BY EXTRACT(HOUR FROM f_ann_date_time);

-- ============================================
-- 3. 重新填充 processed_fundamental_quality
-- ============================================

-- 从 cashflow 表填充（优先级最高）
UPDATE processed_fundamental_quality p
SET f_ann_date_time = c.f_ann_date_time
FROM cashflow c
WHERE p.ts_code = c.ts_code
  AND p.end_date_time = c.end_date_time
  AND c.f_ann_date_time IS NOT NULL;

-- 从 income 表填充
UPDATE processed_fundamental_quality p
SET f_ann_date_time = i.f_ann_date_time
FROM income i
WHERE p.ts_code = i.ts_code
  AND p.end_date_time = i.end_date_time
  AND p.f_ann_date_time IS NULL
  AND i.f_ann_date_time IS NOT NULL;

-- 从 balancesheet 表填充
UPDATE processed_fundamental_quality p
SET f_ann_date_time = b.f_ann_date_time
FROM balancesheet b
WHERE p.ts_code = b.ts_code
  AND p.end_date_time = b.end_date_time
  AND p.f_ann_date_time IS NULL
  AND b.f_ann_date_time IS NOT NULL;

-- 使用 ann_date_time 作为后备
UPDATE processed_fundamental_quality p
SET f_ann_date_time = p.ann_date_time
WHERE p.f_ann_date_time IS NULL
  AND p.ann_date_time IS NOT NULL;

-- 最终后备：使用 end_date_time
UPDATE processed_fundamental_quality p
SET f_ann_date_time = p.end_date_time
WHERE p.f_ann_date_time IS NULL;

-- ============================================
-- 验证
-- ============================================
-- SELECT EXTRACT(HOUR FROM f_ann_date_time) as hr, COUNT(*)
-- FROM processed_fundamental_quality
-- GROUP BY EXTRACT(HOUR FROM f_ann_date_time);