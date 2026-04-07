-- =====================================================
-- Migration 020: Add NDA technical fields and Buffett moat factors
-- =====================================================

-- 1. Technical preprocess tables
ALTER TABLE processed_daily_qfq
    ADD COLUMN IF NOT EXISTS nda_value SMALLINT,
    ADD COLUMN IF NOT EXISTS volume_confirmed BOOLEAN;

ALTER TABLE processed_weekly_qfq
    ADD COLUMN IF NOT EXISTS nda_value SMALLINT,
    ADD COLUMN IF NOT EXISTS volume_confirmed BOOLEAN;

ALTER TABLE processed_monthly_qfq
    ADD COLUMN IF NOT EXISTS nda_value SMALLINT,
    ADD COLUMN IF NOT EXISTS volume_confirmed BOOLEAN;

COMMENT ON COLUMN processed_daily_qfq.nda_value IS '净派发/吸筹方向值';
COMMENT ON COLUMN processed_daily_qfq.volume_confirmed IS '量能方向是否确认';
COMMENT ON COLUMN processed_weekly_qfq.nda_value IS '净派发/吸筹方向值';
COMMENT ON COLUMN processed_weekly_qfq.volume_confirmed IS '量能方向是否确认';
COMMENT ON COLUMN processed_monthly_qfq.nda_value IS '净派发/吸筹方向值';
COMMENT ON COLUMN processed_monthly_qfq.volume_confirmed IS '量能方向是否确认';

-- 2. Quarterly moat factors
ALTER TABLE processed_fundamental_quality
    ADD COLUMN IF NOT EXISTS npm_ttm DECIMAL(20,4),
    ADD COLUMN IF NOT EXISTS gpm_ttm_12q_std DECIMAL(20,4),
    ADD COLUMN IF NOT EXISTS gpm_ttm_12q_delta DECIMAL(20,4),
    ADD COLUMN IF NOT EXISTS npm_ttm_12q_std DECIMAL(20,4),
    ADD COLUMN IF NOT EXISTS cfo_to_ni_ttm DECIMAL(20,4),
    ADD COLUMN IF NOT EXISTS buffett_gpm_flag SMALLINT,
    ADD COLUMN IF NOT EXISTS buffett_npm_stable_flag SMALLINT,
    ADD COLUMN IF NOT EXISTS buffett_roa_flag SMALLINT,
    ADD COLUMN IF NOT EXISTS buffett_cashflow_flag SMALLINT;

COMMENT ON COLUMN processed_fundamental_quality.npm_ttm IS '净利率 TTM，q_netprofit_margin 4期滚动均值';
COMMENT ON COLUMN processed_fundamental_quality.gpm_ttm_12q_std IS '毛利率TTM最近12个季度滚动标准差';
COMMENT ON COLUMN processed_fundamental_quality.gpm_ttm_12q_delta IS '毛利率TTM相对12个季度前的变化值';
COMMENT ON COLUMN processed_fundamental_quality.npm_ttm_12q_std IS '净利率TTM最近12个季度滚动标准差';
COMMENT ON COLUMN processed_fundamental_quality.cfo_to_ni_ttm IS '经营现金流TTM与净利润TTM比值';
COMMENT ON COLUMN processed_fundamental_quality.buffett_gpm_flag IS '巴菲特毛利率护城河标记';
COMMENT ON COLUMN processed_fundamental_quality.buffett_npm_stable_flag IS '巴菲特净利率稳定标记';
COMMENT ON COLUMN processed_fundamental_quality.buffett_roa_flag IS '巴菲特ROA标记';
COMMENT ON COLUMN processed_fundamental_quality.buffett_cashflow_flag IS '巴菲特现金流标记';

-- 3. Extend combined view
CREATE OR REPLACE VIEW v_fundamental_combined AS
SELECT
    fi.time,
    fi.symbol,
    -- Existing valuation columns (keep order for CREATE OR REPLACE compatibility)
    fi.pe_ttm,
    fi.pb,
    fi.ps_ttm,
    fi.dv_ttm,
    fi.peg,
    fi.pe_ttm_pct_1250d,
    fi.pb_pct_1250d,
    fi.ps_ttm_pct_1250d,
    -- Existing quarterly quality columns
    qf.f_score,
    qf.f_roa,
    qf.f_cfo,
    qf.f_delta_roa,
    qf.f_accrual,
    qf.f_delta_lever,
    qf.f_delta_liquid,
    qf.f_eq_offer,
    qf.f_delta_margin,
    qf.f_delta_turn,
    qf.roa_ttm,
    qf.roe_5y_avg,
    qf.ni_cfo_corr_3y,
    qf.debt_ratio,
    qf.current_ratio,
    qf.cfo_ttm,
    qf.ni_ttm,
    qf.gpm_ttm,
    qf.at_ttm,
    qf.exemptions,
    qf.end_date_time AS fscore_period,
    COALESCE(qf.f_ann_date_time, qf.ann_date_time) AS fscore_effective_date,
    -- New Buffett/moat columns must be appended at the tail of the view
    -- to avoid renaming existing columns in CREATE OR REPLACE VIEW.
    qf.npm_ttm,
    qf.gpm_ttm_12q_std,
    qf.gpm_ttm_12q_delta,
    qf.npm_ttm_12q_std,
    qf.cfo_to_ni_ttm,
    qf.buffett_gpm_flag,
    qf.buffett_npm_stable_flag,
    qf.buffett_roa_flag,
    qf.buffett_cashflow_flag
FROM processed_valuation_pct fi
LEFT JOIN LATERAL (
    SELECT *
    FROM processed_fundamental_quality qfi
    WHERE qfi.ts_code = fi.symbol
      AND COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) <= fi.time
    ORDER BY COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) DESC
    LIMIT 1
) qf ON true;

COMMENT ON VIEW v_fundamental_combined IS '日度估值与季度F-Score合并视图,季度数据以公告日为准向前填充';
