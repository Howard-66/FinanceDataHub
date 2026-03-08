-- Migration: Fix f_ann_date_time NULL values in processed_fundamental_quality
-- 从源表填充缺失的 f_ann_date_time

-- ============================================
-- Step 1: 从 cashflow 表填充
-- ============================================
UPDATE processed_fundamental_quality p
SET f_ann_date_time = c.f_ann_date_time
FROM cashflow c
WHERE p.ts_code = c.ts_code
  AND p.end_date_time = c.end_date_time
  AND p.f_ann_date_time IS NULL
  AND c.f_ann_date_time IS NOT NULL;

-- ============================================
-- Step 2: 从 balancesheet 表填充（仍未填充的）
-- ============================================
UPDATE processed_fundamental_quality p
SET f_ann_date_time = b.f_ann_date_time
FROM balancesheet b
WHERE p.ts_code = b.ts_code
  AND p.end_date_time = b.end_date_time
  AND p.f_ann_date_time IS NULL
  AND b.f_ann_date_time IS NOT NULL;

-- ============================================
-- Step 3: 从 income 表填充（仍未填充的）
-- ============================================
UPDATE processed_fundamental_quality p
SET f_ann_date_time = i.f_ann_date_time
FROM income i
WHERE p.ts_code = i.ts_code
  AND p.end_date_time = i.end_date_time
  AND p.f_ann_date_time IS NULL
  AND i.f_ann_date_time IS NOT NULL;

-- ============================================
-- Step 4: 使用 ann_date_time 作为后备（如果源表都没有）
-- ============================================
UPDATE processed_fundamental_quality p
SET f_ann_date_time = p.ann_date_time
WHERE p.f_ann_date_time IS NULL
  AND p.ann_date_time IS NOT NULL;

-- ============================================
-- Step 5: 使用 end_date_time 作为最终后备
-- ============================================
UPDATE processed_fundamental_quality p
SET f_ann_date_time = p.end_date_time
WHERE p.f_ann_date_time IS NULL;

-- ============================================
-- Verification
-- ============================================
-- 检查是否还有 NULL
-- SELECT
--     CASE WHEN f_ann_date_time IS NULL THEN 'NULL' ELSE 'NOT NULL' END as status,
--     COUNT(*) as cnt
-- FROM processed_fundamental_quality
-- GROUP BY CASE WHEN f_ann_date_time IS NULL THEN 'NULL' ELSE 'NOT NULL' END;

-- 检查小时分布
-- SELECT EXTRACT(HOUR FROM f_ann_date_time) as hr, COUNT(*)
-- FROM processed_fundamental_quality
-- GROUP BY EXTRACT(HOUR FROM f_ann_date_time);
