-- =====================================================
-- 添加 F-Score 豁免规则列到行业估值表
-- =====================================================

-- 添加 fscore_exemptions 列（存储行业配置中的 F-Score 豁免规则）
ALTER TABLE processed_industry_valuation
ADD COLUMN IF NOT EXISTS fscore_exemptions JSONB;

-- 添加注释
COMMENT ON COLUMN processed_industry_valuation.fscore_exemptions IS 'F-Score豁免规则JSON数组，如["f_score_cfo", "f_score_leverage"]，来自industry_config.json';

-- 更新表注释
COMMENT ON TABLE processed_industry_valuation IS '行业差异化估值指标表，根据行业配置自动选择核心估值指标，包含F-Score豁免规则';