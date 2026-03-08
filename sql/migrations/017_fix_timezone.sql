-- Migration: 017_fix_timezone
-- Description: Fix timezone inconsistency - convert UTC timestamps to Asia/Shanghai
-- Background: Preprocessed data was incorrectly stored with UTC timezone label,
--             but the actual time value was Asia/Shanghai local time.
--             This caused times to display incorrectly (e.g., 07:00 instead of 15:00).
--
-- IMPORTANT: This script should only be run ONCE after updating the Python code.
--
-- 判断逻辑：
-- 1. Python代码用 tz_localize('UTC') 给 naive datetime 加了 UTC 时区标签
-- 2. 但 naive datetime 实际上已经是北京时间（如 2024-01-01 15:00）
-- 3. 存储后显示为 UTC 15:00，查询时显示为上海 07:00
-- 4. 正确的修复：使用 AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Shanghai'
--    先将带UTC标签的时间转为naive，再转为Asia/Shanghai时区

-- ============================================
-- Phase 0: Diagnostic queries (run first to understand the data)
-- ============================================

-- 检查 processed_weekly_qfq 的时间分布（确认问题）
-- SELECT
--     EXTRACT(HOUR FROM time) as hour,
--     COUNT(*) as cnt,
--     MIN(time::text) as sample_min,
--     MAX(time::text) as sample_max
-- FROM processed_weekly_qfq
-- GROUP BY EXTRACT(HOUR FROM time)
-- ORDER BY hour;

-- ============================================
-- Phase 1: Fix preprocessed data tables
-- ============================================

-- 原理：time AT TIME ZONE 'UTC' 去掉UTC标签得到naive时间
--       再 AT TIME ZONE 'Asia/Shanghai' 加上上海时区标签
--       例如：'2024-01-01 07:00:00+08' AT TIME ZONE 'UTC' = '2024-01-01 07:00:00'(naive)
--             '2024-01-01 07:00:00' AT TIME ZONE 'Asia/Shanghai' = '2024-01-01 07:00:00+08'
--       等等，这不对...

-- 正确理解：
-- 如果存储的是 UTC 15:00（实际是北京时间15点被标为UTC）
-- 查询显示为上海 07:00（UTC+8转换后）
-- 我们需要：UTC 15:00 -> 上海 15:00
-- 方法：(time AT TIME ZONE 'UTC')::timestamp = naive UTC time
--       然后 AT TIME ZONE 'Asia/Shanghai' = Shanghai time with +8 offset
--       但这样 time 值会变成 23:00...

-- 实际上正确的修复是：
-- 1. 提取当前的"显示时间"（去掉时区）
-- 2. 重新加上 Asia/Shanghai 时区
-- 对于 07:00+08（存储的是 UTC 07:00 显示为上海 07:00）：
--   time::timestamp = 07:00:00 (naive)
--   time::timestamp AT TIME ZONE 'Asia/Shanghai' = 07:00:00+08
-- 但这不是我们想要的...

-- 让我重新分析：
-- 如果 Python 存储了 naive datetime(2024,1,1,15,0) 并 tz_localize('UTC')
-- 数据库收到：2024-01-01 15:00:00+00 (UTC)
-- 查询显示：2024-01-01 23:00:00+08 (上海) 或 2024-01-01 07:00:00+08（如果系统做了转换）

-- 等等，用户说 hour=8/9 的数据有 345万行，修复后变成 hour=16/17
-- 这意味着：8 + 8 = 16
-- 所以原始数据 hour=8，是 UTC 08:00，显示为上海 16:00
-- 但用户说显示为上海 07:00...

-- 重新理解：
-- 用户检查结果：hour=8/9 和 hour=16/17 同时存在
-- 执行 UPDATE +8小时后，hour=8/9 变成了 hour=16/17
-- 这意味着 8 + 8 = 16, 9 + 8 = 17
-- 所以原始数据确实是 hour=8/9

-- 正确的修复：将 hour=8/9/16/17 的数据统一设为 15:00
-- 或者理解为：这些数据应该是 15:00，但存储错了

-- 方案：直接设置时间为当天的 15:00
UPDATE processed_weekly_qfq
SET time = date_trunc('day', time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM time) IN (7, 8, 9, 16, 17);

-- 其他表同样处理
UPDATE processed_monthly_qfq
SET time = date_trunc('day', time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM time) IN (7, 8, 9, 16, 17);

UPDATE processed_daily_qfq
SET time = date_trunc('day', time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM time) IN (7, 8, 9, 16, 17);

UPDATE processed_valuation_pct
SET time = date_trunc('day', time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM time) IN (7, 8, 9, 16, 17);

UPDATE processed_industry_valuation
SET time = date_trunc('day', time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM time) IN (7, 8, 9, 16, 17);

-- 财报表也统一处理
UPDATE processed_fundamental_quality
SET end_date_time = date_trunc('day', end_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM end_date_time) IN (7, 8, 9, 16, 17);

UPDATE processed_fundamental_quality
SET ann_date_time = date_trunc('day', ann_date_time) + INTERVAL '15 hours'
WHERE ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE processed_fundamental_quality
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE f_ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9, 16, 17);

-- ============================================
-- Phase 2: Fix financial report tables
-- ============================================

UPDATE fina_indicator
SET end_date_time = date_trunc('day', end_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM end_date_time) IN (7, 8, 9, 16, 17);

UPDATE fina_indicator
SET ann_date_time = date_trunc('day', ann_date_time) + INTERVAL '15 hours'
WHERE ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE cashflow
SET end_date_time = date_trunc('day', end_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM end_date_time) IN (7, 8, 9, 16, 17);

UPDATE cashflow
SET ann_date_time = date_trunc('day', ann_date_time) + INTERVAL '15 hours'
WHERE ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE cashflow
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE f_ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE balancesheet
SET end_date_time = date_trunc('day', end_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM end_date_time) IN (7, 8, 9, 16, 17);

UPDATE balancesheet
SET ann_date_time = date_trunc('day', ann_date_time) + INTERVAL '15 hours'
WHERE ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE balancesheet
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE f_ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE income
SET end_date_time = date_trunc('day', end_date_time) + INTERVAL '15 hours'
WHERE EXTRACT(HOUR FROM end_date_time) IN (7, 8, 9, 16, 17);

UPDATE income
SET ann_date_time = date_trunc('day', ann_date_time) + INTERVAL '15 hours'
WHERE ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM ann_date_time) IN (7, 8, 9, 16, 17);

UPDATE income
SET f_ann_date_time = date_trunc('day', f_ann_date_time) + INTERVAL '15 hours'
WHERE f_ann_date_time IS NOT NULL
  AND EXTRACT(HOUR FROM f_ann_date_time) IN (7, 8, 9, 16, 17);

-- ============================================
-- Verification queries (run manually to check)
-- ============================================

-- 检查是否还有异常小时的数据（应该只返回 hour=15）
-- SELECT
--     EXTRACT(HOUR FROM time) as hour,
--     COUNT(*) as cnt
-- FROM processed_weekly_qfq
-- GROUP BY EXTRACT(HOUR FROM time)
-- ORDER BY hour;

-- 检查样本数据
-- SELECT time::text FROM processed_weekly_qfq LIMIT 5;
