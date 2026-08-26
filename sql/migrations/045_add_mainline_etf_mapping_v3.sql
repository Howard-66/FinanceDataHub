-- AlphaLine v3: auditable point-in-time ETF benchmark mappings.
--
-- etf_basic.index_code is a current catalogue attribute.  It must never be
-- applied retroactively without a reviewed historical evidence record.
BEGIN;

CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS mainline_etf_benchmark_history (
  ts_code VARCHAR(32) NOT NULL,
  benchmark_index_code VARCHAR(32),
  effective_from_date DATE NOT NULL,
  effective_to_date DATE,
  usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,
  mapping_status VARCHAR(32) NOT NULL
    CHECK (mapping_status IN ('mapped','mapping_pending','ambiguous_multisector','not_applicable')),
  source_name VARCHAR(64) NOT NULL,
  evidence_reference TEXT,
  confidence VARCHAR(16) NOT NULL
    CHECK (confidence IN ('high','medium','low')),
  review_status VARCHAR(16) NOT NULL
    CHECK (review_status IN ('pending','approved','rejected')),
  reviewed_at TIMESTAMPTZ,
  reviewed_by VARCHAR(128),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ts_code, usable_from_trade_date),
  CHECK (effective_to_date IS NULL OR effective_to_date >= effective_from_date),
  CHECK (usable_to_trade_date IS NULL OR usable_to_trade_date > usable_from_trade_date),
  CHECK (
    (mapping_status = 'mapped' AND benchmark_index_code IS NOT NULL AND review_status = 'approved')
    OR mapping_status <> 'mapped'
  )
);

ALTER TABLE mainline_etf_benchmark_history
  DROP CONSTRAINT IF EXISTS mainline_etf_benchmark_history_no_overlapping_usable_ranges;
ALTER TABLE mainline_etf_benchmark_history
  ADD CONSTRAINT mainline_etf_benchmark_history_no_overlapping_usable_ranges
  EXCLUDE USING gist (
    ts_code WITH =,
    daterange(usable_from_trade_date, COALESCE(usable_to_trade_date, 'infinity'::date), '[)') WITH &&
  );

CREATE INDEX IF NOT EXISTS idx_mainline_etf_benchmark_history_lookup
  ON mainline_etf_benchmark_history(ts_code, usable_from_trade_date, usable_to_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_benchmark_history_review
  ON mainline_etf_benchmark_history(review_status, mapping_status);

-- Seed records are deliberately pending.  They are an operator work queue,
-- not historical facts and therefore cannot be consumed by factor version 3.
INSERT INTO mainline_etf_benchmark_history(
  ts_code, benchmark_index_code, effective_from_date, usable_from_trade_date,
  mapping_status, source_name, evidence_reference, confidence, review_status
)
SELECT
  ts_code, index_code, COALESCE(list_date, setup_date, DATE '1900-01-01'),
  COALESCE(list_date, setup_date, DATE '1900-01-01'),
  'mapping_pending',
  'etf_basic', 'Seeded from current etf_basic.index_code; review required', 'low', 'pending'
FROM etf_basic
ON CONFLICT (ts_code, usable_from_trade_date) DO NOTHING;

ALTER TABLE processed_mainline_etf_daily
  ADD COLUMN IF NOT EXISTS benchmark_mapping_status VARCHAR(32),
  ADD COLUMN IF NOT EXISTS benchmark_mapping_source VARCHAR(64),
  ADD COLUMN IF NOT EXISTS benchmark_mapping_confidence VARCHAR(16),
  ADD COLUMN IF NOT EXISTS benchmark_mapping_review_status VARCHAR(16);

CREATE INDEX IF NOT EXISTS idx_mainline_etf_mapping_status
  ON processed_mainline_etf_daily(factor_version, trade_date, benchmark_mapping_status);

CREATE OR REPLACE VIEW v_mainline_etf_mapping_coverage AS
SELECT
  b.ts_code,
  b.list_date,
  b.index_code AS catalog_index_code,
  COALESCE(h.mapping_status, 'mapping_pending') AS mapping_status,
  COALESCE(h.review_status, 'pending') AS review_status,
  h.benchmark_index_code,
  h.usable_from_trade_date,
  h.usable_to_trade_date,
  h.source_name,
  h.confidence,
  h.evidence_reference,
  h.reviewed_at,
  EXISTS(SELECT 1 FROM etf_index i WHERE i.ts_code = h.benchmark_index_code) AS etf_index_catalog_match,
  EXISTS(SELECT 1 FROM mkt_idx_bmk m WHERE m.ts_code = h.benchmark_index_code OR m.symbol = h.benchmark_index_code) AS benchmark_catalog_match,
  EXISTS(SELECT 1 FROM fund_basic f WHERE f.ts_code = b.ts_code) AS fund_catalog_match
FROM etf_basic b
LEFT JOIN mainline_etf_benchmark_history h ON h.ts_code = b.ts_code;

CREATE OR REPLACE VIEW v_mainline_etf_whitelist_sensitivity AS
SELECT
  factor_version,
  trade_date,
  COUNT(*) AS etf_count,
  COUNT(*) FILTER (WHERE benchmark_mapping_status = 'mapped') AS mapped_count,
  COUNT(*) FILTER (WHERE benchmark_mapping_status = 'mapping_pending') AS mapping_pending_count,
  COUNT(*) FILTER (WHERE benchmark_mapping_status = 'ambiguous_multisector') AS ambiguous_multisector_count,
  COUNT(*) FILTER (WHERE benchmark_mapping_status = 'not_applicable') AS not_applicable_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped') AS mapped_tradable_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND avg_amount_20d >= 30000000) AS liquidity_pass_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND total_size >= 500000000) AS aum_pass_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND premium_discount_abs_p95_60d <= 0.02) AS premium_pass_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND tracking_error_60d <= 0.03) AS tracking_error_pass_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND primary_l2_weight >= 0.50) AS exposure_pass_count,
  COUNT(*) FILTER (WHERE data_complete AND is_tradable AND benchmark_mapping_status = 'mapped'
    AND avg_amount_20d >= 30000000 AND total_size >= 500000000
    AND premium_discount_abs_p95_60d <= 0.02 AND tracking_error_60d <= 0.03
    AND primary_l2_weight >= 0.50) AS full_whitelist_count
FROM processed_mainline_etf_daily
GROUP BY factor_version, trade_date;

COMMENT ON TABLE mainline_etf_benchmark_history IS
  'Reviewed point-in-time ETF to benchmark-index mappings for AlphaLine v3.';
COMMENT ON VIEW v_mainline_etf_mapping_coverage IS
  'Operator audit view for ETF benchmark mapping status and evidence.';
COMMENT ON VIEW v_mainline_etf_whitelist_sensitivity IS
  'Daily v3 ETF mapping and fixed-whitelist sensitivity counts.';

COMMIT;
