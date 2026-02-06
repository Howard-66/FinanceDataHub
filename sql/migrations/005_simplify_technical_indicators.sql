-- =====================================================
-- 简化技术指标和基本面指标列
-- 1. 移除不需要的 MA 和 RSI 列，添加 MA_50
-- 2. 移除 1/2/3 年估值分位，添加 10 年估值分位
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

-- 4. 处理 fundamental_indicators 表
ALTER TABLE fundamental_indicators
DROP COLUMN IF EXISTS pe_ttm_pct_250d,
DROP COLUMN IF EXISTS pb_pct_250d,
DROP COLUMN IF EXISTS ps_ttm_pct_250d,
DROP COLUMN IF EXISTS pe_ttm_pct_500d,
DROP COLUMN IF EXISTS pb_pct_500d,
DROP COLUMN IF EXISTS ps_ttm_pct_500d,
DROP COLUMN IF EXISTS pe_ttm_pct_750d,
DROP COLUMN IF EXISTS pb_pct_750d,
DROP COLUMN IF EXISTS ps_ttm_pct_750d;

ALTER TABLE fundamental_indicators
ADD COLUMN IF NOT EXISTS pe_ttm_pct_2500d DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS pb_pct_2500d DECIMAL(10,4),
ADD COLUMN IF NOT EXISTS ps_ttm_pct_2500d DECIMAL(10,4);
