-- Migration Script: 移除 symbol_daily 表中未使用的字段
-- 执行时机: Phase 4 完成后
-- 说明: Tushare daily 接口不返回 adj_factor、open_interest、settle 字段
--       复权因子已存储在独立的 adj_factor 表中

-- 0. 查找所有与 symbol_daily 相关的连续聚合
SELECT viewname FROM pg_views WHERE schemaname = 'public' AND viewname LIKE '%symbol_daily%';

-- 1. 删除与 symbol_daily 相关的连续聚合视图
DROP MATERIALIZED VIEW IF EXISTS symbol_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS symbol_monthly CASCADE;

-- 2. 移除未使用的字段（使用 CASCADE 移除 TimescaleDB 内部视图依赖）
ALTER TABLE symbol_daily DROP COLUMN IF EXISTS adj_factor CASCADE;
ALTER TABLE symbol_daily DROP COLUMN IF EXISTS open_interest CASCADE;
ALTER TABLE symbol_daily DROP COLUMN IF EXISTS settle CASCADE;

-- 3. 验证修改
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'symbol_daily'
ORDER BY ordinal_position;

-- 4. 可选：重新创建连续聚合（如果需要）
-- SELECT create_continuous_aggregate('symbol_daily', 'symbol_weekly', INTERVAL '1 week');
-- SELECT create_continuous_aggregate('symbol_daily', 'symbol_monthly', INTERVAL '1 month');

-- 回滚脚本（如需回滚）：
-- 注意: 回滚需要重新创建被级联删除的视图，这需要从 003_create_hypertables.sql 重新执行
-- ALTER TABLE symbol_daily ADD COLUMN adj_factor DECIMAL(20,10);
-- ALTER TABLE symbol_daily ADD COLUMN open_interest BIGINT;
-- ALTER TABLE symbol_daily ADD COLUMN settle DECIMAL(20,6);


-- 移除 adj_factor 表中的 adj_type 字段
ALTER TABLE public.adj_factor DROP COLUMN adj_type;