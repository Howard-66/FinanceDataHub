-- AlphaLine Gate 0 support for mainline factor formula v2.
-- This migration is additive: published v1 rows remain untouched.
BEGIN;

ALTER TABLE processed_mainline_etf_daily
  ADD COLUMN IF NOT EXISTS premium_discount_abs_p95_60d NUMERIC(24,10);

COMMENT ON COLUMN processed_mainline_etf_daily.premium_discount_abs_p95_60d IS
  'PIT-safe rolling 60-observation 95th percentile of absolute ETF premium/discount.';
COMMENT ON COLUMN processed_mainline_etf_daily.amount IS
  'ETF trading amount normalized to CNY for factor_version >= 2.';
COMMENT ON COLUMN processed_mainline_etf_daily.total_size IS
  'ETF AUM normalized to CNY for factor_version >= 2.';
COMMENT ON COLUMN processed_mainline_stock_daily.amount IS
  'Stock trading amount normalized to CNY for factor_version >= 2.';
COMMENT ON COLUMN processed_mainline_stock_daily.revenue_acceleration IS
  'Current disclosed quarter YoY minus the previous disclosed quarter YoY; PIT safe for factor_version >= 2.';

CREATE INDEX IF NOT EXISTS idx_mainline_manifest_v2_status_usable
  ON processed_mainline_snapshot_manifest
  (factor_version, status, usable_from_trade_date DESC);
CREATE INDEX IF NOT EXISTS idx_mainline_status_v2_partition
  ON processed_mainline_data_status
  (factor_version, partition_date DESC, dataset);

COMMIT;
