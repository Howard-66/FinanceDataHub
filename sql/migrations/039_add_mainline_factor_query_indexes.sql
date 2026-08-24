-- Run only after 038 and a successful full backfill.  Do not wrap this file
-- in BEGIN/COMMIT: each TimescaleDB parent-table index propagates to hundreds
-- of chunks, and releasing locks statement-by-statement keeps the shared lock
-- table bounded.  IF NOT EXISTS makes a partial prior run safe to resume.
CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_date ON processed_mainline_stock_daily(factor_version, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_version_code_date ON processed_mainline_stock_daily(factor_version, ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_usable ON processed_mainline_stock_daily(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_market_version_date ON processed_mainline_market_daily(factor_version, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_market_usable ON processed_mainline_market_daily(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_date ON processed_mainline_industry_daily(factor_version, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_version_code_date ON processed_mainline_industry_daily(factor_version, l2_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_usable ON processed_mainline_industry_daily(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_date ON processed_mainline_etf_daily(factor_version, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_version_code_date ON processed_mainline_etf_daily(factor_version, ts_code, trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_usable ON processed_mainline_etf_daily(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_exposure_lookup ON processed_mainline_etf_exposure_monthly(factor_version, ts_code, as_of_trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_exposure_usable ON processed_mainline_etf_exposure_monthly(usable_from_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_fund_crowding_usable ON processed_mainline_fund_crowding_monthly(factor_version, usable_from_trade_date, ts_code);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_crowding_usable ON processed_mainline_industry_crowding_monthly(factor_version, usable_from_trade_date, l2_code);
CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_lookup ON processed_mainline_leadlag_monthly(factor_version, month_end DESC, follower_code);
CREATE INDEX IF NOT EXISTS idx_mainline_leadlag_score_lookup ON processed_mainline_leadlag_score_monthly(factor_version, month_end DESC, l2_code);
CREATE INDEX IF NOT EXISTS idx_mainline_manifest_ready ON processed_mainline_snapshot_manifest(status, usable_from_trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_status_lookup ON processed_mainline_data_status(factor_version, partition_date DESC, status);

-- Compression is enabled only where this TimescaleDB version exposes the legacy
-- API.  Newer installations may use columnstore policies externally; indexes
-- remain valid either way.
DO $$
DECLARE t text;
BEGIN
  IF to_regprocedure('add_compression_policy(regclass,interval)') IS NOT NULL THEN
    FOREACH t IN ARRAY ARRAY[
      'processed_mainline_stock_daily','processed_mainline_market_daily','processed_mainline_industry_daily','processed_mainline_etf_daily',
      'processed_mainline_etf_exposure_monthly','processed_mainline_fund_crowding_monthly','processed_mainline_industry_crowding_monthly',
      'processed_mainline_leadlag_monthly','processed_mainline_leadlag_score_monthly'
    ] LOOP
      EXECUTE format('ALTER TABLE %I SET (timescaledb.compress, timescaledb.compress_segmentby = ''factor_version'')', t);
      PERFORM add_compression_policy(t::regclass, INTERVAL '30 days', if_not_exists => TRUE);
    END LOOP;
  END IF;
END $$;

CREATE OR REPLACE VIEW v_mainline_ready_snapshot AS
SELECT * FROM processed_mainline_snapshot_manifest WHERE status = 'ready';
