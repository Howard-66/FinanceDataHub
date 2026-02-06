-- =====================================================
-- 简化技术指标列
-- 移除不需要的 MA 和 RSI 列，添加 MA_50
-- =====================================================

-- 1. 处理 processed_daily_qfq 表
ALTER TABLE processed_daily_qfq 
DROP COLUMN IF EXISTS ma_5,
DROP COLUMN IF EXISTS ma_10,
DROP COLUMN IF EXISTS ma_60,
DROP COLUMN IF EXISTS ma_120,
DROP COLUMN IF EXISTS ma_250,
DROP COLUMN IF EXISTS rsi_6;

ALTER TABLE processed_daily_qfq 
ADD COLUMN IF NOT EXISTS ma_50 DECIMAL(20,6);

-- 2. 处理 processed_weekly_qfq 表
ALTER TABLE processed_weekly_qfq 
DROP COLUMN IF EXISTS ma_5,
DROP COLUMN IF EXISTS ma_10,
DROP COLUMN IF EXISTS rsi_6;

ALTER TABLE processed_weekly_qfq 
ADD COLUMN IF NOT EXISTS ma_50 DECIMAL(20,6);

-- 3. 处理 processed_monthly_qfq 表
ALTER TABLE processed_monthly_qfq 
DROP COLUMN IF EXISTS ma_5,
DROP COLUMN IF EXISTS ma_10,
DROP COLUMN IF EXISTS rsi_6;

ALTER TABLE processed_monthly_qfq 
ADD COLUMN IF NOT EXISTS ma_50 DECIMAL(20,6);
