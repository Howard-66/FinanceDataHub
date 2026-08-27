-- AlphaLine v4 PIT factors and hierarchical ETF mapping contract.
BEGIN;

ALTER TABLE processed_mainline_industry_daily
  ADD COLUMN IF NOT EXISTS relative_strength_slope_20d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS risk_adjusted_momentum_60d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS information_ratio_60d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS path_efficiency_60d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS tail_return_p05_60d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS top5_amount_share_change_20d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS crowding_score NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS price_gap_20d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS turnover_ratio_20d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS up_amount_concentration_40d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS price_volume_corr_change_20d NUMERIC(24,10);

ALTER TABLE processed_mainline_etf_daily
  ADD COLUMN IF NOT EXISTS volatility_60d NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS mapping_level VARCHAR(32),
  ADD COLUMN IF NOT EXISTS target_coverage NUMERIC(24,10),
  ADD COLUMN IF NOT EXISTS non_target_l1_exposure NUMERIC(24,10),
  -- Timescale columnstore hypertables only allow constant-free ADD COLUMN.
  -- v4 materialization always writes both values; legacy factor versions may
  -- retain NULL because they do not participate in the v4 mapping policy.
  ADD COLUMN IF NOT EXISTS exposure_vector JSONB,
  ADD COLUMN IF NOT EXISTS mapping_evidence JSONB;

CREATE TABLE IF NOT EXISTS mainline_etf_strategy_mapping_history (
  ts_code VARCHAR(32) NOT NULL,
  effective_from_date DATE NOT NULL,
  effective_to_date DATE,
  usable_from_trade_date DATE NOT NULL,
  usable_to_trade_date DATE,
  mapping_level VARCHAR(32) NOT NULL CHECK (mapping_level IN ('exact_l2','l1_proxy','theme_proxy')),
  target_l2_codes TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  target_l1_code VARCHAR(30),
  target_coverage NUMERIC(24,10) NOT NULL CHECK (target_coverage >= 0 AND target_coverage <= 1),
  non_target_l1_exposure NUMERIC(24,10) NOT NULL DEFAULT 0 CHECK (non_target_l1_exposure >= 0 AND non_target_l1_exposure <= 1),
  exposure_vector JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence_reference TEXT NOT NULL,
  reviewed_by VARCHAR(128) NOT NULL,
  reviewed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (ts_code, usable_from_trade_date),
  CHECK (effective_to_date IS NULL OR effective_to_date >= effective_from_date),
  CHECK (usable_to_trade_date IS NULL OR usable_to_trade_date > usable_from_trade_date)
);

CREATE EXTENSION IF NOT EXISTS btree_gist;
ALTER TABLE mainline_etf_strategy_mapping_history
  DROP CONSTRAINT IF EXISTS mainline_etf_strategy_mapping_no_overlapping_usable_ranges;
ALTER TABLE mainline_etf_strategy_mapping_history
  ADD CONSTRAINT mainline_etf_strategy_mapping_no_overlapping_usable_ranges
  EXCLUDE USING gist (
    ts_code WITH =,
    daterange(usable_from_trade_date, COALESCE(usable_to_trade_date, 'infinity'::date), '[)') WITH &&
  );

CREATE INDEX IF NOT EXISTS idx_mainline_etf_strategy_mapping_lookup
  ON mainline_etf_strategy_mapping_history(ts_code, usable_from_trade_date, usable_to_trade_date);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_v4_momentum
  ON processed_mainline_industry_daily(factor_version, trade_date, l2_code);

COMMENT ON TABLE mainline_etf_strategy_mapping_history IS
  'Reviewed PIT L2/L1/theme ETF mapping evidence for AlphaLine v4; no inferred proxy is permitted.';

COMMIT;
