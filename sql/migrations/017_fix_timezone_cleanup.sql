-- Migration: 017_fix_timezone_cleanup
-- Description: 清理时区修复过程中的异常数据
-- 处理主键冲突：删除 hour=0/7/8/9 的错误数据，保留 hour=15 的正确数据

-- ============================================
-- 对于 processed_fundamental_quality 表
-- 由于 hour=0 的数据更新为 15:00 会导致主键冲突
-- 直接删除这些错误数据（正确的数据已存在于 hour=15）
-- ============================================

-- 删除 end_date_time 为 hour=0/7/8/9/16/17 的数据（错误数据）
DELETE FROM processed_fundamental_quality
WHERE EXTRACT(HOUR FROM end_date_time) IN (0, 7, 8, 9, 16, 17);

-- 验证：应该只剩 hour=15
-- SELECT EXTRACT(HOUR FROM end_date_time) as hr, COUNT(*)
-- FROM processed_fundamental_quality
-- GROUP BY EXTRACT(HOUR FROM end_date_time);

-- ============================================
-- 验证其他预处理表是否全部修复为 hour=15
-- ============================================

-- 检查 processed_weekly_qfq
-- SELECT EXTRACT(HOUR FROM time) as hr, COUNT(*) FROM processed_weekly_qfq GROUP BY EXTRACT(HOUR FROM time);

-- 检查 processed_monthly_qfq
-- SELECT EXTRACT(HOUR FROM time) as hr, COUNT(*) FROM processed_monthly_qfq GROUP BY EXTRACT(HOUR FROM time);

-- 检查 processed_daily_qfq
-- SELECT EXTRACT(HOUR FROM time) as hr, COUNT(*) FROM processed_daily_qfq GROUP BY EXTRACT(HOUR FROM time);

-- 检查 processed_valuation_pct
-- SELECT EXTRACT(HOUR FROM time) as hr, COUNT(*) FROM processed_valuation_pct GROUP BY EXTRACT(HOUR FROM time);

-- 检查 processed_industry_valuation
-- SELECT EXTRACT(HOUR FROM time) as hr, COUNT(*) FROM processed_industry_valuation GROUP BY EXTRACT(HOUR FROM time);

-- ============================================
-- 验证财报表
-- ============================================

-- 检查 fina_indicator
-- SELECT EXTRACT(HOUR FROM end_date_time) as hr, COUNT(*) FROM fina_indicator GROUP BY EXTRACT(HOUR FROM end_date_time);

-- 检查 cashflow
-- SELECT EXTRACT(HOUR FROM end_date_time) as hr, COUNT(*) FROM cashflow GROUP BY EXTRACT(HOUR FROM end_date_time);

-- 检查 balancesheet
-- SELECT EXTRACT(HOUR FROM end_date_time) as hr, COUNT(*) FROM balancesheet GROUP BY EXTRACT(HOUR FROM end_date_time);

-- 检查 income
-- SELECT EXTRACT(HOUR FROM end_date_time) as hr, COUNT(*) FROM income GROUP BY EXTRACT(HOUR FROM end_date_time);
