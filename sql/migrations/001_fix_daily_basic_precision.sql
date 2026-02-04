-- Migration: Fix daily_basic table precision for turnover_rate and volume_ratio
-- Date: 2026-02-05
-- Issue: numeric field overflow - A field with precision 10, scale 6 must round to an absolute value less than 10^4
-- This migration handles the continuous aggregates that depend on these columns

-- Step 1: Remove the continuous aggregate policies
SELECT remove_continuous_aggregate_policy('daily_basic_weekly', if_exists => true);
SELECT remove_continuous_aggregate_policy('daily_basic_monthly', if_exists => true);

-- Step 2: Drop the continuous aggregates (materialized views)
DROP MATERIALIZED VIEW IF EXISTS daily_basic_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS daily_basic_monthly CASCADE;

-- Step 3: Alter the column types
ALTER TABLE daily_basic ALTER COLUMN turnover_rate TYPE DECIMAL(20,6);
ALTER TABLE daily_basic ALTER COLUMN volume_ratio TYPE DECIMAL(20,6);

-- Step 4: Recreate the continuous aggregates

-- daily_basic_weekly
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_basic_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_basic_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

CREATE INDEX IF NOT EXISTS idx_daily_basic_weekly_symbol_time ON daily_basic_weekly (symbol, time DESC);
COMMENT ON VIEW daily_basic_weekly IS '周线基础指标聚合表 - 存储预计算的周线每日基础指标';

-- daily_basic_monthly
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_basic_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
  symbol,
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_basic_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

CREATE INDEX IF NOT EXISTS idx_daily_basic_monthly_symbol_time ON daily_basic_monthly (symbol, time DESC);
COMMENT ON VIEW daily_basic_monthly IS '月线基础指标聚合表 - 存储预计算的月线每日基础指标';

-- Verify the changes
SELECT column_name, data_type, numeric_precision, numeric_scale 
FROM information_schema.columns 
WHERE table_name = 'daily_basic' 
AND column_name IN ('turnover_rate', 'volume_ratio');
