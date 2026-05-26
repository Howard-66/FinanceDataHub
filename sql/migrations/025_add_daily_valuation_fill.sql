-- =====================================================
-- Migration 025: Add derived daily valuation fill layer
-- =====================================================

CREATE TABLE IF NOT EXISTS processed_daily_valuation_fill (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(20) NOT NULL,

    pe DECIMAL(20,4),
    pe_ttm DECIMAL(20,4),
    pb DECIMAL(20,4),
    ps DECIMAL(20,4),
    ps_ttm DECIMAL(20,4),
    peg DECIMAL(20,4),
    dv_ratio DECIMAL(20,4),
    dv_ttm DECIMAL(20,4),

    sources JSONB NOT NULL DEFAULT '{}'::jsonb,
    denominator_dates JSONB NOT NULL DEFAULT '{}'::jsonb,
    quality_flags JSONB NOT NULL DEFAULT '{}'::jsonb,
    formula_version VARCHAR(64) NOT NULL DEFAULT 'valuation_fill_v1',
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable('processed_daily_valuation_fill', 'time',
    if_not_exists => TRUE,
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 month'
);

CREATE INDEX IF NOT EXISTS idx_daily_valuation_fill_symbol
    ON processed_daily_valuation_fill (symbol);
CREATE INDEX IF NOT EXISTS idx_daily_valuation_fill_time
    ON processed_daily_valuation_fill (time DESC);

COMMENT ON TABLE processed_daily_valuation_fill IS '日度估值缺失补值表，保存由财报和市值推导的估值，不覆盖daily_basic原始数据';
COMMENT ON COLUMN processed_daily_valuation_fill.sources IS '各指标补值来源JSON，如{"pe_ttm":"derived_ttm_income.n_income_attr_p"}';
COMMENT ON COLUMN processed_daily_valuation_fill.denominator_dates IS '各指标使用的财报报告期JSON';
COMMENT ON COLUMN processed_daily_valuation_fill.quality_flags IS '补值质量标记JSON';
COMMENT ON COLUMN processed_daily_valuation_fill.formula_version IS '补值公式版本';

CREATE OR REPLACE VIEW v_daily_basic_enriched AS
SELECT
    d.time,
    d.symbol,
    d.turnover_rate,
    d.volume_ratio,
    COALESCE(d.pe, f.pe) AS pe,
    COALESCE(d.pe_ttm, f.pe_ttm) AS pe_ttm,
    COALESCE(d.pb, f.pb) AS pb,
    COALESCE(d.ps, f.ps) AS ps,
    COALESCE(d.ps_ttm, f.ps_ttm) AS ps_ttm,
    f.peg AS peg,
    COALESCE(d.dv_ratio, f.dv_ratio) AS dv_ratio,
    COALESCE(d.dv_ttm, f.dv_ttm) AS dv_ttm,
    d.total_share,
    d.float_share,
    d.free_share,
    d.total_mv,
    d.circ_mv,
    CASE WHEN d.pe IS NOT NULL THEN 'daily_basic' WHEN f.pe IS NOT NULL THEN COALESCE(f.sources->>'pe', 'valuation_fill') END AS pe_source,
    CASE WHEN d.pe_ttm IS NOT NULL THEN 'daily_basic' WHEN f.pe_ttm IS NOT NULL THEN COALESCE(f.sources->>'pe_ttm', 'valuation_fill') END AS pe_ttm_source,
    CASE WHEN d.pb IS NOT NULL THEN 'daily_basic' WHEN f.pb IS NOT NULL THEN COALESCE(f.sources->>'pb', 'valuation_fill') END AS pb_source,
    CASE WHEN d.ps IS NOT NULL THEN 'daily_basic' WHEN f.ps IS NOT NULL THEN COALESCE(f.sources->>'ps', 'valuation_fill') END AS ps_source,
    CASE WHEN d.ps_ttm IS NOT NULL THEN 'daily_basic' WHEN f.ps_ttm IS NOT NULL THEN COALESCE(f.sources->>'ps_ttm', 'valuation_fill') END AS ps_ttm_source,
    CASE WHEN f.peg IS NOT NULL THEN COALESCE(f.sources->>'peg', 'valuation_fill') END AS peg_source,
    CASE WHEN d.dv_ratio IS NOT NULL THEN 'daily_basic' WHEN f.dv_ratio IS NOT NULL THEN COALESCE(f.sources->>'dv_ratio', 'valuation_fill') END AS dv_ratio_source,
    CASE WHEN d.dv_ttm IS NOT NULL THEN 'daily_basic' WHEN f.dv_ttm IS NOT NULL THEN COALESCE(f.sources->>'dv_ttm', 'valuation_fill') END AS dv_ttm_source,
    f.sources AS valuation_fill_sources,
    f.denominator_dates AS valuation_fill_denominator_dates,
    f.quality_flags AS valuation_fill_quality_flags,
    f.formula_version AS valuation_fill_formula_version
FROM daily_basic d
LEFT JOIN processed_daily_valuation_fill f
  ON f.symbol = d.symbol AND f.time = d.time;

COMMENT ON VIEW v_daily_basic_enriched IS 'daily_basic原始估值与内部派生补值的合并视图，逐字段优先使用Tushare原始值';
