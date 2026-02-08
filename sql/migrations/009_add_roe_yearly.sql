-- Migration: Add roe_yearly column to fina_indicator table
-- Description: 添加年化净资产收益率字段，用于计算5年平均ROE

BEGIN;

-- 添加 roe_yearly 列
ALTER TABLE fina_indicator 
ADD COLUMN IF NOT EXISTS roe_yearly DECIMAL(10,4);

-- 添加注释
COMMENT ON COLUMN fina_indicator.roe_yearly IS '年化净资产收益率(%)';

COMMIT;
