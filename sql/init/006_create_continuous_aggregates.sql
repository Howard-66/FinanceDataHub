-- 连续聚合初始化脚本
-- 创建高周期数据聚合（周线、月线）用于分析查询
-- 使用 TimescaleDB Continuous Aggregates 实现自动维护的物化视图

-- ============================================================================
-- 1. symbol_weekly - 周线 OHLCV 数据聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS symbol_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  -- 复权价格：使用期间最后一天的复权因子统一调整期间内所有价格
  first(open, time) * last(adj_factor, time) / first(adj_factor, time) AS open,
  max(high) * last(adj_factor, time) / first(adj_factor, time) AS high,
  min(low) * last(adj_factor, time) / first(adj_factor, time) AS low,
  last(close, time) AS close,
  sum(volume) AS volume,
  sum(amount) AS amount,
  last(adj_factor, time) AS adj_factor
FROM symbol_daily
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

-- 周线聚合刷新策略：每 1 小时刷新，覆盖 1 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('symbol_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_symbol_weekly_symbol_time ON symbol_weekly (symbol, time DESC);

COMMENT ON VIEW symbol_weekly IS '周线数据聚合表 - 存储预计算的周线 OHLCV 数据，自动维护复权价格';

-- ============================================================================
-- 2. symbol_monthly - 月线 OHLCV 数据聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS symbol_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
  symbol,
  -- 复权价格：使用期间最后一天的复权因子统一调整期间内所有价格
  first(open, time) * last(adj_factor, time) / first(adj_factor, time) AS open,
  max(high) * last(adj_factor, time) / first(adj_factor, time) AS high,
  min(low) * last(adj_factor, time) / first(adj_factor, time) AS low,
  last(close, time) AS close,
  sum(volume) AS volume,
  sum(amount) AS amount,
  last(adj_factor, time) AS adj_factor
FROM symbol_daily
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

-- 月线聚合刷新策略：每 1 小时刷新，覆盖 3 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('symbol_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_symbol_monthly_symbol_time ON symbol_monthly (symbol, time DESC);

COMMENT ON VIEW symbol_monthly IS '月线数据聚合表 - 存储预计算的月线 OHLCV 数据，自动维护复权价格';

-- ============================================================================
-- 3. daily_basic_weekly - 周线每日基础指标聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_basic_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  -- 比率指标取平均值
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  -- 股本和市值取期末值（时点快照）
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

-- 周线基础指标刷新策略：每 1 小时刷新，覆盖 1 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('daily_basic_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_daily_basic_weekly_symbol_time ON daily_basic_weekly (symbol, time DESC);

COMMENT ON VIEW daily_basic_weekly IS '周线基础指标聚合表 - 存储预计算的周线每日基础指标';

-- ============================================================================
-- 4. daily_basic_monthly - 月线每日基础指标聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS daily_basic_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
  symbol,
  -- 比率指标取平均值
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  -- 股本和市值取期末值（时点快照）
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

-- 月线基础指标刷新策略：每 1 小时刷新，覆盖 3 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('daily_basic_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_daily_basic_monthly_symbol_time ON daily_basic_monthly (symbol, time DESC);

COMMENT ON VIEW daily_basic_monthly IS '月线基础指标聚合表 - 存储预计算的月线每日基础指标';

-- ============================================================================
-- 5. adj_factor_weekly - 周线复权因子聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS adj_factor_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  -- 复权因子取期末值（表示该周结束时的复权因子）
  last(adj_factor, time) AS adj_factor
FROM adj_factor
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

-- 周线复权因子刷新策略：每 1 小时刷新，覆盖 1 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('adj_factor_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_adj_factor_weekly_symbol_time ON adj_factor_weekly (symbol, time DESC);

COMMENT ON VIEW adj_factor_weekly IS '周线复权因子聚合表 - 存储预计算的周线复权因子数据';

-- ============================================================================
-- 6. adj_factor_monthly - 月线复权因子聚合
-- ============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS adj_factor_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
  symbol,
  -- 复权因子取期末值（表示该月结束时的复权因子）
  last(adj_factor, time) AS adj_factor
FROM adj_factor
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

-- 月线复权因子刷新策略：每 1 小时刷新，覆盖 3 个月历史，带 1 小时滞后
SELECT add_continuous_aggregate_policy('adj_factor_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建查询优化索引
CREATE INDEX IF NOT EXISTS idx_adj_factor_monthly_symbol_time ON adj_factor_monthly (symbol, time DESC);

COMMENT ON VIEW adj_factor_monthly IS '月线复权因子聚合表 - 存储预计算的月线复权因子数据';

-- ============================================================================
-- 完成信息
-- ============================================================================

-- 验证连续聚合创建情况（可以通过查询验证）
-- SELECT view_name FROM timescaledb_information.continuous_aggregates
-- WHERE view_name IN ('symbol_weekly', 'symbol_monthly', 'daily_basic_weekly', 'daily_basic_monthly', 'adj_factor_weekly', 'adj_factor_monthly')
-- ORDER BY view_name;
