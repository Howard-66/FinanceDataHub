-- Rebuild point-in-time mainline factor tables.  Raw source tables are never
-- touched by this migration.  Run with psql -v ON_ERROR_STOP=1.
BEGIN;

-- Refuse to drop a table if an object outside the mainline family depends on it.
-- This is deliberately stricter than DROP ... CASCADE: an unexpected consumer
-- must be migrated explicitly instead of disappearing silently.
DO $$
DECLARE dependent_object text;
BEGIN
  SELECT format('%I.%I (%s)', n.nspname, c.relname, c.relkind)
    INTO dependent_object
  FROM pg_depend d
  JOIN pg_class referenced ON referenced.oid = d.refobjid
  JOIN pg_namespace referenced_ns ON referenced_ns.oid = referenced.relnamespace
  JOIN pg_class c ON c.oid = d.objid
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE referenced_ns.nspname = current_schema()
    AND referenced.relname LIKE 'processed_mainline_%'
    AND n.nspname = current_schema()
    AND c.relname NOT LIKE 'processed_mainline_%'
    AND c.relkind IN ('v','m','r','p','f')
  LIMIT 1;
  IF dependent_object IS NOT NULL THEN
    RAISE EXCEPTION 'refusing mainline rebuild: non-mainline dependency found: %', dependent_object;
  END IF;
END $$;

DROP TABLE IF EXISTS processed_mainline_leadlag_score_monthly;
DROP TABLE IF EXISTS processed_mainline_leadlag_monthly;
DROP TABLE IF EXISTS processed_mainline_industry_crowding_monthly;
DROP TABLE IF EXISTS processed_mainline_fund_crowding_monthly;
DROP TABLE IF EXISTS processed_mainline_etf_exposure_monthly;
DROP TABLE IF EXISTS processed_mainline_etf_daily;
DROP TABLE IF EXISTS processed_mainline_industry_daily;
DROP TABLE IF EXISTS processed_mainline_market_daily;
DROP TABLE IF EXISTS processed_mainline_stock_daily;
DROP TABLE IF EXISTS processed_mainline_snapshot_manifest;
DROP TABLE IF EXISTS processed_mainline_data_status;

-- factor_version is part of every identity.  A published historical snapshot is
-- immutable: formula changes create another version rather than overwriting it.
CREATE TABLE processed_mainline_stock_daily (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  trade_date DATE NOT NULL, as_of_trade_date DATE NOT NULL,
  usable_from_trade_date DATE NOT NULL,
  ts_code VARCHAR(20) NOT NULL,
  l1_code VARCHAR(30), l1_name VARCHAR(100), l2_code VARCHAR(30), l2_name VARCHAR(100),
  is_listed BOOLEAN, is_st BOOLEAN, st_source VARCHAR(32), is_suspended BOOLEAN,
  is_market_breadth_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  is_industry_breadth_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  is_stock_candidate_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  is_eligible BOOLEAN NOT NULL DEFAULT FALSE,
  exclusion_reason TEXT, exclusion_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  listing_days INTEGER, trading_days_20d INTEGER,
  close NUMERIC(24,8), amount NUMERIC(30,6), total_mv NUMERIC(30,6), circ_mv NUMERIC(30,6),
  circ_mv_pct NUMERIC(24,10), turnover_rate NUMERIC(24,8),
  avg_amount_20d NUMERIC(30,6), avg_amount_60d NUMERIC(30,6), amount_ratio_20_60 NUMERIC(24,10), amount_pct_20d NUMERIC(24,10),
  turnover_pct_2y NUMERIC(24,10), turnover_pct_20d NUMERIC(24,10),
  pe_ttm NUMERIC(24,8), pb NUMERIC(24,8), dv_ttm NUMERIC(24,8), pe_pct_5y NUMERIC(24,10), pb_pct_5y NUMERIC(24,10),
  ma60_gap NUMERIC(24,10), ma120_gap NUMERIC(24,10), ma200_gap NUMERIC(24,10),
  return_20d NUMERIC(24,10), return_60d NUMERIC(24,10), return_120d NUMERIC(24,10),
  volatility_20d NUMERIC(24,10), volatility_60d NUMERIC(24,10), volatility_120d NUMERIC(24,10),
  drawdown_120d NUMERIC(24,10), path_efficiency_60d NUMERIC(24,10), tail_return_p05_60d NUMERIC(24,10),
  roe_ttm NUMERIC(24,8), roa_ttm NUMERIC(24,8), roic NUMERIC(24,8), grossprofit_margin NUMERIC(24,8),
  revenue_yoy NUMERIC(24,8), profit_yoy NUMERIC(24,8), revenue_yoy_prev NUMERIC(24,8), profit_yoy_prev NUMERIC(24,8),
  revenue_acceleration NUMERIC(24,8), profit_acceleration NUMERIC(24,8), ocf_to_profit NUMERIC(24,8), debt_to_assets NUMERIC(24,8),
  financial_available_date DATE, dividend_event_120d BOOLEAN, repurchase_event_120d BOOLEAN,
  dividend_yield_ttm NUMERIC(24,10), shareholder_return NUMERIC(24,10), earnings_surprise_proxy NUMERIC(24,10),
  rzye NUMERIC(30,6), rqye NUMERIC(30,6), rzmre NUMERIC(30,6),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, ts_code, trade_date)
);
SELECT create_hypertable('processed_mainline_stock_daily', 'trade_date', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_market_daily (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  trade_date DATE NOT NULL, as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  benchmark_code VARCHAR(32) NOT NULL DEFAULT '000985.CSI', benchmark_close NUMERIC(24,8),
  benchmark_return_20d NUMERIC(24,10), benchmark_return_60d NUMERIC(24,10), benchmark_return_120d NUMERIC(24,10),
  benchmark_ma20_gap NUMERIC(24,10), benchmark_ma60_gap NUMERIC(24,10), benchmark_ma200_gap NUMERIC(24,10), benchmark_volatility_20d NUMERIC(24,10),
  breadth_above_ma20 NUMERIC(24,10), breadth_above_ma60 NUMERIC(24,10), breadth_above_ma120 NUMERIC(24,10),
  breadth_denominator INTEGER, breadth_above_ma60_count INTEGER, breadth_above_ma120_count INTEGER, effective_stock_count INTEGER,
  advance_decline_ratio NUMERIC(24,10), north_money NUMERIC(24,6), north_money_20d NUMERIC(30,6), market_regime VARCHAR(32),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, trade_date)
);
SELECT create_hypertable('processed_mainline_market_daily', 'trade_date', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_industry_daily (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  trade_date DATE NOT NULL, as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  l1_code VARCHAR(30), l1_name VARCHAR(100), l2_code VARCHAR(30) NOT NULL, l2_name VARCHAR(100),
  index_code VARCHAR(32), index_close NUMERIC(24,8), stock_count INTEGER,
  equal_weight_return NUMERIC(24,10), cap_weight_return NUMERIC(24,10),
  return_20d NUMERIC(24,10), return_60d NUMERIC(24,10), return_120d NUMERIC(24,10),
  relative_return_20d NUMERIC(24,10), relative_return_60d NUMERIC(24,10), relative_return_120d NUMERIC(24,10),
  ma60_gap NUMERIC(24,10), ma120_gap NUMERIC(24,10), breadth_above_ma20 NUMERIC(24,10), breadth_above_ma60 NUMERIC(24,10), breadth_above_ma120 NUMERIC(24,10),
  strong_stock_count INTEGER, strong_stock_ratio NUMERIC(24,10), return_dispersion NUMERIC(24,10), amount_share NUMERIC(24,10),
  avg_amount_20d NUMERIC(30,6), avg_amount_60d NUMERIC(30,6), amount_ratio_20_60 NUMERIC(24,10), top5_amount_share NUMERIC(24,10),
  median_pe_ttm NUMERIC(24,8), median_pb NUMERIC(24,8), median_roe_ttm NUMERIC(24,8), median_roic NUMERIC(24,8), median_grossprofit_margin NUMERIC(24,8),
  median_revenue_yoy NUMERIC(24,8), median_profit_yoy NUMERIC(24,8), revenue_acceleration NUMERIC(24,8), profit_acceleration NUMERIC(24,8),
  margin_balance_change_20d NUMERIC(30,6), fund_holding_value NUMERIC(30,6), fund_count INTEGER, fund_holding_change NUMERIC(30,6), fund_concentration NUMERIC(24,10), disclosure_coverage NUMERIC(24,10),
  etf_net_inflow_20d NUMERIC(30,6), etf_aum NUMERIC(30,6), crowding_input NUMERIC(24,10),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, l2_code, trade_date)
);
SELECT create_hypertable('processed_mainline_industry_daily', 'trade_date', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_etf_daily (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  trade_date DATE NOT NULL, as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  ts_code VARCHAR(32) NOT NULL, index_code VARCHAR(32), list_date DATE,
  benchmark_available BOOLEAN NOT NULL DEFAULT FALSE, data_complete BOOLEAN NOT NULL DEFAULT FALSE, is_tradable BOOLEAN NOT NULL DEFAULT FALSE,
  is_eligible BOOLEAN NOT NULL DEFAULT FALSE, exclusion_reason TEXT, exclusion_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  adj_close NUMERIC(24,8), ma60_gap NUMERIC(24,10), return_20d NUMERIC(24,10), return_60d NUMERIC(24,10), return_120d NUMERIC(24,10), volatility_20d NUMERIC(24,10),
  amount NUMERIC(30,6), avg_amount_20d NUMERIC(30,6), avg_amount_60d NUMERIC(30,6), amount_ratio_20_60 NUMERIC(24,10), amount_pct_20d NUMERIC(24,10),
  total_share NUMERIC(30,6), total_size NUMERIC(30,6), share_change_5d NUMERIC(30,6), share_change_20d NUMERIC(30,6), net_inflow_5d NUMERIC(30,6), net_inflow_20d NUMERIC(30,6),
  tracking_error_60d NUMERIC(24,10), tracking_error_120d NUMERIC(24,10), premium_discount NUMERIC(24,10),
  primary_l2_code VARCHAR(30), primary_l2_weight NUMERIC(24,10), top5_l2_exposure JSONB NOT NULL DEFAULT '[]'::jsonb, exposure_hhi NUMERIC(24,10),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, ts_code, trade_date)
);
SELECT create_hypertable('processed_mainline_etf_daily', 'trade_date', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_etf_exposure_monthly (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL, weight_date DATE NOT NULL,
  ts_code VARCHAR(32) NOT NULL, l2_code VARCHAR(30) NOT NULL, weight NUMERIC(24,10) NOT NULL,
  mapping_method VARCHAR(32) NOT NULL, is_primary BOOLEAN NOT NULL DEFAULT FALSE, top5_rank SMALLINT,
  exposure_hhi NUMERIC(24,10), data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, ts_code, l2_code, as_of_trade_date)
);
SELECT create_hypertable('processed_mainline_etf_exposure_monthly', 'as_of_trade_date', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_fund_crowding_monthly (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  report_period DATE NOT NULL, as_of_trade_date DATE NOT NULL, available_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  ts_code VARCHAR(20) NOT NULL, fund_count INTEGER, holding_value NUMERIC(30,6), holding_ratio NUMERIC(24,10), crowding_pct NUMERIC(24,10),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, ts_code, report_period)
);
SELECT create_hypertable('processed_mainline_fund_crowding_monthly', 'report_period', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_industry_crowding_monthly (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  report_period DATE NOT NULL, as_of_trade_date DATE NOT NULL, available_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  l2_code VARCHAR(30) NOT NULL, holding_value NUMERIC(30,6), fund_count INTEGER, holding_change NUMERIC(30,6), concentration NUMERIC(24,10), disclosure_coverage NUMERIC(24,10),
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, l2_code, report_period)
);
SELECT create_hypertable('processed_mainline_industry_crowding_monthly', 'report_period', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_leadlag_monthly (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  month_end DATE NOT NULL, as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  leader_type VARCHAR(16) NOT NULL DEFAULT 'industry', leader_code VARCHAR(32) NOT NULL,
  follower_type VARCHAR(16) NOT NULL DEFAULT 'industry', follower_code VARCHAR(32) NOT NULL,
  best_lag_days INTEGER, correlation NUMERIC(24,10), regression_coef NUMERIC(24,10), selected BOOLEAN NOT NULL DEFAULT FALSE,
  sample_count INTEGER, training_start_date DATE, training_end_date DATE, stability_first NUMERIC(24,10), stability_second NUMERIC(24,10), stability_third NUMERIC(24,10),
  model_version VARCHAR(64) NOT NULL DEFAULT 'lasso-post-lasso-v1', data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, month_end, leader_code, follower_code)
);
SELECT create_hypertable('processed_mainline_leadlag_monthly', 'month_end', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_leadlag_score_monthly (
  factor_version SMALLINT NOT NULL DEFAULT 1,
  month_end DATE NOT NULL, as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL,
  l2_code VARCHAR(30) NOT NULL, leadlag_score NUMERIC(24,10), predicted_relative_return_20d NUMERIC(24,10), selected_feature_count INTEGER,
  training_start_date DATE, training_end_date DATE, model_version VARCHAR(64) NOT NULL DEFAULT 'lasso-post-lasso-v1',
  data_quality JSONB NOT NULL DEFAULT '{}'::jsonb, source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_asof TIMESTAMPTZ NOT NULL DEFAULT NOW(), processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, month_end, l2_code)
);
SELECT create_hypertable('processed_mainline_leadlag_score_monthly', 'month_end', if_not_exists => TRUE, migrate_data => FALSE);

CREATE TABLE processed_mainline_snapshot_manifest (
  factor_version SMALLINT NOT NULL, snapshot_id UUID NOT NULL,
  as_of_trade_date DATE NOT NULL, usable_from_trade_date DATE NOT NULL, status VARCHAR(16) NOT NULL,
  formula_hash VARCHAR(128) NOT NULL, input_watermark JSONB NOT NULL DEFAULT '{}'::jsonb, coverage JSONB NOT NULL DEFAULT '{}'::jsonb,
  blocker_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[], published_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, as_of_trade_date)
);

CREATE TABLE processed_mainline_data_status (
  factor_version SMALLINT NOT NULL DEFAULT 1, dataset VARCHAR(64) NOT NULL, partition_date DATE NOT NULL,
  row_count BIGINT NOT NULL DEFAULT 0, eligible_count BIGINT, excluded_count BIGINT, max_source_date DATE, completeness NUMERIC(12,8),
  status VARCHAR(16) NOT NULL, blocker_reasons TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[], details JSONB NOT NULL DEFAULT '{}'::jsonb,
  source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb, checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (factor_version, dataset, partition_date)
);

COMMENT ON TABLE processed_mainline_snapshot_manifest IS 'PIT manifest. ready snapshots are read by strategy/backtest only from usable_from_trade_date.';
COMMENT ON TABLE processed_mainline_data_status IS 'Latest completed-trading-day quality gate; no weekend zero-row stale records.';
COMMIT;
