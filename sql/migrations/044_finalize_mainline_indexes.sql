-- Run only after a successful v2 rebuild and Gate 0 validation.
DO $$ DECLARE t text; BEGIN
  FOREACH t IN ARRAY ARRAY[
    'processed_mainline_stock_daily','processed_mainline_market_daily',
    'processed_mainline_industry_daily','processed_mainline_etf_daily',
    'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly',
    'processed_mainline_industry_crowding_monthly','processed_mainline_leadlag_monthly',
    'processed_mainline_leadlag_score_monthly'
  ] LOOP
    EXECUTE format('ALTER TABLE %I RESET (autovacuum_enabled)',t);
  END LOOP;
END $$;

CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_date
  ON processed_mainline_stock_daily(factor_version,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_code_date
  ON processed_mainline_stock_daily(factor_version,ts_code,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_usable
  ON processed_mainline_stock_daily(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_market_version_date
  ON processed_mainline_market_daily(factor_version,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_date
  ON processed_mainline_industry_daily(factor_version,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_code_date
  ON processed_mainline_industry_daily(factor_version,l2_code,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_date
  ON processed_mainline_etf_daily(factor_version,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_code_date
  ON processed_mainline_etf_daily(factor_version,ts_code,trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_exposure_lookup
  ON processed_mainline_etf_exposure_monthly(factor_version,ts_code,as_of_trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_fund_crowding_usable
  ON processed_mainline_fund_crowding_monthly(factor_version,usable_from_trade_date,ts_code);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_crowding_usable
  ON processed_mainline_industry_crowding_monthly(factor_version,usable_from_trade_date,l2_code);
CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_lookup
  ON processed_mainline_leadlag_monthly(factor_version,month_end DESC,follower_code);
CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_score_lookup
  ON processed_mainline_leadlag_score_monthly(factor_version,month_end DESC,l2_code);
CREATE INDEX IF NOT EXISTS idx_mainline_manifest_ready
  ON processed_mainline_snapshot_manifest(factor_version,status,usable_from_trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_status_lookup
  ON processed_mainline_data_status(factor_version,partition_date DESC,status);

CREATE OR REPLACE VIEW v_mainline_ready_snapshot AS
SELECT * FROM processed_mainline_snapshot_manifest WHERE status='ready';

ANALYZE processed_mainline_stock_daily;
ANALYZE processed_mainline_market_daily;
ANALYZE processed_mainline_industry_daily;
ANALYZE processed_mainline_etf_daily;
ANALYZE processed_mainline_etf_exposure_monthly;
ANALYZE processed_mainline_fund_crowding_monthly;
ANALYZE processed_mainline_industry_crowding_monthly;
ANALYZE processed_mainline_leadlag_monthly;
ANALYZE processed_mainline_leadlag_score_monthly;

DO $$
DECLARE item record;
BEGIN
  IF EXISTS(SELECT 1 FROM pg_proc WHERE proname='add_compression_policy') THEN
    FOR item IN SELECT * FROM (VALUES
      ('processed_mainline_stock_daily','factor_version,ts_code','trade_date DESC'),
      ('processed_mainline_etf_daily','factor_version,ts_code','trade_date DESC'),
      ('processed_mainline_market_daily','factor_version','trade_date DESC'),
      ('processed_mainline_industry_daily','factor_version,l2_code','trade_date DESC'),
      ('processed_mainline_etf_exposure_monthly','factor_version,ts_code','as_of_trade_date DESC'),
      ('processed_mainline_fund_crowding_monthly','factor_version,ts_code','report_period DESC'),
      ('processed_mainline_industry_crowding_monthly','factor_version,l2_code','report_period DESC'),
      ('processed_mainline_leadlag_monthly','factor_version,follower_code','month_end DESC'),
      ('processed_mainline_leadlag_score_monthly','factor_version,l2_code','month_end DESC')
    ) AS v(table_name,segment_by,order_by) LOOP
      EXECUTE format(
        'ALTER TABLE %I SET (timescaledb.compress=true,timescaledb.compress_segmentby=%L,timescaledb.compress_orderby=%L)',
        item.table_name,item.segment_by,item.order_by
      );
      BEGIN
        PERFORM add_compression_policy(item.table_name::regclass,INTERVAL '180 days',if_not_exists=>TRUE);
      EXCEPTION WHEN undefined_function OR feature_not_supported THEN
        RAISE NOTICE 'compression policy API unavailable for %',item.table_name;
      END;
    END LOOP;
  END IF;
END $$;
