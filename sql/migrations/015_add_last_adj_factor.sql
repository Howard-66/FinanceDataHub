-- =====================================================
-- 迁移 015: 添加 last_adj_factor 列到预处理表
-- =====================================================
-- 用于智能增量预处理优化：检测 adj_factor 是否变化
-- 仅当 adj_factor 变化时才需要全量重算该股票

-- 1. processed_daily_qfq 表
ALTER TABLE processed_daily_qfq
    ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);

-- 2. processed_weekly_qfq 表
ALTER TABLE processed_weekly_qfq
    ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);

-- 3. processed_monthly_qfq 表
ALTER TABLE processed_monthly_qfq
    ADD COLUMN IF NOT EXISTS last_adj_factor DECIMAL(20,10);

-- 添加注释说明
COMMENT ON COLUMN processed_daily_qfq.last_adj_factor IS '该批次数据对应的最新复权因子，用于检测是否需要全量重算';
COMMENT ON COLUMN processed_weekly_qfq.last_adj_factor IS '该批次数据对应的最新复权因子，用于检测是否需要全量重算';
COMMENT ON COLUMN processed_monthly_qfq.last_adj_factor IS '该批次数据对应的最新复权因子，用于检测是否需要全量重算';
