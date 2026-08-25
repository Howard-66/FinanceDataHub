-- Destructive performance rebuild for the reproducible mainline factor layer.
-- Raw facts and the non-mainline preprocessing tables are never touched.
-- Run with psql -v ON_ERROR_STOP=1 after capturing the v1 validation sample.
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;
BEGIN;

DO $$
DECLARE dependency text;
BEGIN
  SELECT format('%I.%I', dependent_ns.nspname, dependent.relname)
    INTO dependency
  FROM pg_depend d
  JOIN pg_class source ON source.oid=d.refobjid
  JOIN pg_namespace source_ns ON source_ns.oid=source.relnamespace
  JOIN pg_class dependent ON dependent.oid=d.objid
  JOIN pg_namespace dependent_ns ON dependent_ns.oid=dependent.relnamespace
  WHERE source_ns.nspname=current_schema()
    AND source.relname LIKE 'processed_mainline_%'
    AND dependent.relkind IN ('v','m','r','p','f')
    AND dependent_ns.nspname=current_schema()
    AND dependent.relname NOT LIKE 'processed_mainline_%'
    AND dependent.relname<>'v_mainline_ready_snapshot'
  LIMIT 1;
  IF dependency IS NOT NULL THEN
    RAISE EXCEPTION 'refusing mainline rebuild: external dependency %',dependency;
  END IF;
END $$;

DROP VIEW IF EXISTS v_mainline_ready_snapshot;

-- Renaming first lets LIKE preserve the complete v2 schema assembled by
-- migrations 038-041 without copying any of the 22M+ derived rows.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'processed_mainline_stock_daily','processed_mainline_market_daily',
    'processed_mainline_industry_daily','processed_mainline_etf_daily',
    'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly',
    'processed_mainline_industry_crowding_monthly','processed_mainline_leadlag_monthly',
    'processed_mainline_leadlag_score_monthly','processed_mainline_snapshot_manifest',
    'processed_mainline_data_status'
  ] LOOP
    IF to_regclass(t) IS NULL THEN
      RAISE EXCEPTION 'required table % is missing; apply migrations 038-041 first',t;
    END IF;
    EXECUTE format('ALTER TABLE %I RENAME TO %I',t,t||'__v1_drop');
    EXECUTE format(
      'CREATE TABLE %I (LIKE %I INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE INCLUDING COMMENTS)',
      t,t||'__v1_drop'
    );
    EXECUTE format('ALTER TABLE %I ALTER COLUMN factor_version SET DEFAULT 2',t);
  END LOOP;
END $$;

-- Derived analytics do not need arbitrary precision. DOUBLE PRECISION cuts
-- sort/window memory and storage while preserving more precision than the API
-- exposes. Raw accounting facts retain their original exact types.
DO $$
DECLARE c record;
BEGIN
  FOR c IN
    SELECT table_name,column_name
    FROM information_schema.columns
    WHERE table_schema=current_schema()
      AND table_name LIKE 'processed_mainline_%'
      AND table_name NOT LIKE '%__v1_drop'
      AND data_type='numeric'
  LOOP
    EXECUTE format(
      'ALTER TABLE %I ALTER COLUMN %I TYPE DOUBLE PRECISION USING %I::double precision',
      c.table_name,c.column_name,c.column_name
    );
  END LOOP;
END $$;

-- Internal reuse fields keep market/industry aggregation from repeating
-- stock-level short rolling windows over the entire history.
ALTER TABLE processed_mainline_stock_daily
  ADD COLUMN IF NOT EXISTS return_1d DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS ma20_gap DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS margin_balance_change_20d DOUBLE PRECISION;

ALTER TABLE processed_mainline_stock_daily
  ADD PRIMARY KEY(factor_version,ts_code,trade_date);
ALTER TABLE processed_mainline_market_daily
  ADD PRIMARY KEY(factor_version,trade_date);
ALTER TABLE processed_mainline_industry_daily
  ADD PRIMARY KEY(factor_version,l2_code,trade_date);
ALTER TABLE processed_mainline_etf_daily
  ADD PRIMARY KEY(factor_version,ts_code,trade_date);
ALTER TABLE processed_mainline_etf_exposure_monthly
  ADD PRIMARY KEY(factor_version,ts_code,l2_code,as_of_trade_date);
ALTER TABLE processed_mainline_fund_crowding_monthly
  ADD PRIMARY KEY(factor_version,ts_code,report_period);
ALTER TABLE processed_mainline_industry_crowding_monthly
  ADD PRIMARY KEY(factor_version,l2_code,report_period);
ALTER TABLE processed_mainline_leadlag_monthly
  ADD PRIMARY KEY(factor_version,month_end,leader_code,follower_code);
ALTER TABLE processed_mainline_leadlag_score_monthly
  ADD PRIMARY KEY(factor_version,month_end,l2_code);
ALTER TABLE processed_mainline_snapshot_manifest
  ADD PRIMARY KEY(factor_version,as_of_trade_date);
ALTER TABLE processed_mainline_data_status
  ADD PRIMARY KEY(factor_version,dataset,partition_date);

SELECT create_hypertable('processed_mainline_stock_daily','trade_date',
  chunk_time_interval=>INTERVAL '90 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_etf_daily','trade_date',
  chunk_time_interval=>INTERVAL '90 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_market_daily','trade_date',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_industry_daily','trade_date',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_etf_exposure_monthly','as_of_trade_date',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_fund_crowding_monthly','report_period',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_industry_crowding_monthly','report_period',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_leadlag_monthly','month_end',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);
SELECT create_hypertable('processed_mainline_leadlag_score_monthly','month_end',
  chunk_time_interval=>INTERVAL '365 days',if_not_exists=>TRUE,migrate_data=>FALSE);

DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY[
    'processed_mainline_stock_daily','processed_mainline_market_daily',
    'processed_mainline_industry_daily','processed_mainline_etf_daily',
    'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly',
    'processed_mainline_industry_crowding_monthly','processed_mainline_leadlag_monthly',
    'processed_mainline_leadlag_score_monthly'
  ] LOOP
    EXECUTE format('ALTER TABLE %I SET (autovacuum_enabled=false)',t);
  END LOOP;
END $$;

CREATE TABLE processed_mainline_build_run(
  run_id UUID PRIMARY KEY,
  factor_version SMALLINT NOT NULL,
  formula_hash VARCHAR(128) NOT NULL,
  requested_start_date DATE NOT NULL,
  requested_end_date DATE NOT NULL,
  status VARCHAR(16) NOT NULL CHECK(status IN ('running','completed','failed')),
  source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  error_message TEXT,
  UNIQUE(factor_version,formula_hash,requested_start_date,requested_end_date)
);

CREATE TABLE processed_mainline_build_stage(
  run_id UUID NOT NULL REFERENCES processed_mainline_build_run(run_id) ON DELETE CASCADE,
  stage VARCHAR(32) NOT NULL,
  partition_start DATE NOT NULL,
  partition_end DATE NOT NULL,
  status VARCHAR(16) NOT NULL CHECK(status IN ('running','completed','failed')),
  row_count BIGINT NOT NULL DEFAULT 0,
  duration_seconds DOUBLE PRECISION,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ,
  error_message TEXT,
  PRIMARY KEY(run_id,stage,partition_start,partition_end)
);

CREATE TABLE processed_mainline_financial_pit(
  ts_code VARCHAR(20) NOT NULL,
  usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,
  report_period DATE,
  roe_ttm DOUBLE PRECISION,roa_ttm DOUBLE PRECISION,roic DOUBLE PRECISION,
  grossprofit_margin DOUBLE PRECISION,revenue_yoy DOUBLE PRECISION,
  profit_yoy DOUBLE PRECISION,revenue_yoy_prev DOUBLE PRECISION,
  profit_yoy_prev DOUBLE PRECISION,revenue_acceleration DOUBLE PRECISION,
  profit_acceleration DOUBLE PRECISION,ocf_to_profit DOUBLE PRECISION,
  debt_to_assets DOUBLE PRECISION,
  source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(ts_code,usable_from_trade_date)
);
CREATE INDEX idx_mainline_financial_pit_lookup
  ON processed_mainline_financial_pit(ts_code,usable_from_trade_date,usable_to_trade_date);

CREATE TABLE processed_mainline_sw_member_pit(
  ts_code VARCHAR(20) NOT NULL,usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,l1_code VARCHAR(30),l1_name VARCHAR(100),
  l2_code VARCHAR(30) NOT NULL,l2_name VARCHAR(100),
  PRIMARY KEY(ts_code,usable_from_trade_date,l2_code)
);
CREATE INDEX idx_mainline_sw_member_pit_lookup
  ON processed_mainline_sw_member_pit(ts_code,usable_from_trade_date,usable_to_trade_date);

CREATE TABLE processed_mainline_stock_status_pit(
  ts_code VARCHAR(20) NOT NULL,usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,is_st BOOLEAN NOT NULL DEFAULT FALSE,
  st_source VARCHAR(32) NOT NULL,source_name VARCHAR(100),
  PRIMARY KEY(ts_code,usable_from_trade_date)
);
CREATE INDEX idx_mainline_stock_status_pit_lookup
  ON processed_mainline_stock_status_pit(ts_code,usable_from_trade_date,usable_to_trade_date);

CREATE TABLE processed_mainline_stock_event_pit(
  ts_code VARCHAR(20) NOT NULL,event_type VARCHAR(24) NOT NULL,
  usable_from_trade_date DATE NOT NULL,usable_to_trade_date DATE NOT NULL,
  PRIMARY KEY(ts_code,event_type,usable_from_trade_date)
);
CREATE INDEX idx_mainline_stock_event_pit_lookup
  ON processed_mainline_stock_event_pit(ts_code,event_type,usable_from_trade_date,usable_to_trade_date);

CREATE TABLE processed_mainline_etf_exposure_summary(
  factor_version SMALLINT NOT NULL DEFAULT 2,ts_code VARCHAR(32) NOT NULL,
  as_of_trade_date DATE NOT NULL,usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,primary_l2_code VARCHAR(30),
  primary_l2_weight DOUBLE PRECISION,top5_l2_exposure JSONB NOT NULL DEFAULT '[]'::jsonb,
  exposure_hhi DOUBLE PRECISION,source_watermark JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY(factor_version,ts_code,as_of_trade_date)
);
CREATE INDEX idx_mainline_etf_exposure_summary_lookup
  ON processed_mainline_etf_exposure_summary(factor_version,ts_code,usable_from_trade_date,usable_to_trade_date);

-- The old hypertables contain only reproducible v1/partial-v2 rows and were
-- explicitly approved for deletion. No CASCADE is used.
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY[
    'processed_mainline_stock_daily','processed_mainline_market_daily',
    'processed_mainline_industry_daily','processed_mainline_etf_daily',
    'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly',
    'processed_mainline_industry_crowding_monthly','processed_mainline_leadlag_monthly',
    'processed_mainline_leadlag_score_monthly','processed_mainline_snapshot_manifest',
    'processed_mainline_data_status'
  ] LOOP
    EXECUTE format('DROP TABLE %I',t||'__v1_drop');
  END LOOP;
END $$;

COMMIT;
