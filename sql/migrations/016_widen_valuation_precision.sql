-- ==========================================
-- 迁移 016: 扩大估值和基本面指标表的数值精度
-- 
-- 问题: DECIMAL(10,4) 最大值约 10^6，部分股票的 PE_TTM
--       等极端值会超出范围导致 NumericValueOutOfRangeError
-- 方案: 将所有 DECIMAL(10,4) 列升级为 DECIMAL(20,4)
-- ==========================================

-- =====================================================
-- 1. processed_valuation_pct 表（日度估值分位）
-- =====================================================

ALTER TABLE processed_valuation_pct
    ALTER COLUMN pe_ttm TYPE DECIMAL(20,4),
    ALTER COLUMN pb TYPE DECIMAL(20,4),
    ALTER COLUMN ps_ttm TYPE DECIMAL(20,4),
    ALTER COLUMN dv_ttm TYPE DECIMAL(20,4),
    ALTER COLUMN peg TYPE DECIMAL(20,4),
    ALTER COLUMN pe_ttm_pct_1250d TYPE DECIMAL(20,4),
    ALTER COLUMN pb_pct_1250d TYPE DECIMAL(20,4),
    ALTER COLUMN ps_ttm_pct_1250d TYPE DECIMAL(20,4);

-- =====================================================
-- 2. processed_fundamental_quality 表（季度基本面指标）
-- =====================================================

ALTER TABLE processed_fundamental_quality
    ALTER COLUMN roa_ttm TYPE DECIMAL(20,4),
    ALTER COLUMN roe_5y_avg TYPE DECIMAL(20,4),
    ALTER COLUMN ni_cfo_corr_3y TYPE DECIMAL(20,4),
    ALTER COLUMN debt_ratio TYPE DECIMAL(20,4),
    ALTER COLUMN current_ratio TYPE DECIMAL(20,4),
    ALTER COLUMN gpm_ttm TYPE DECIMAL(20,4),
    ALTER COLUMN at_ttm TYPE DECIMAL(20,4);
