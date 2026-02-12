-- =====================================================
-- 添加股息率 TTM 字段到估值分位表
-- =====================================================

-- 添加 dv_ttm 字段到 processed_valuation_pct 表
ALTER TABLE processed_valuation_pct
    ADD COLUMN IF NOT EXISTS dv_ttm DECIMAL(10,4),                     -- 股息率(TTM)

-- 添加列注释
COMMENT ON COLUMN processed_valuation_pct.dv_ttm IS '股息率(TTM)，税前股息率';

-- 更新合并视图，添加 dv_ttm 字段
CREATE OR REPLACE VIEW v_fundamental_combined AS
SELECT
    fi.time,
    fi.symbol,
    -- 日度估值指标
    fi.pe_ttm,
    fi.pb,
    fi.ps_ttm,
    fi.dv_ttm,
    fi.peg,
    fi.pe_ttm_pct_1250d,
    fi.pb_pct_1250d,
    fi.ps_ttm_pct_1250d,
    -- 季度 F-Score 指标 (forward-fill)
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
    -- 季度补充指标
    qf.roa_ttm,
    qf.roe_5y_avg,
    qf.ni_cfo_corr_3y,
    qf.debt_ratio,
    qf.current_ratio,
    -- TTM 指标
    qf.cfo_ttm,
    qf.ni_ttm,
    qf.gpm_ttm,
    qf.at_ttm,
    qf.exemptions,
    -- 季度数据元信息
    qf.end_date_time AS fscore_period,
    COALESCE(qf.f_ann_date_time, qf.ann_date_time) AS fscore_effective_date
FROM processed_valuation_pct fi
LEFT JOIN LATERAL (
    SELECT * FROM processed_fundamental_quality qfi
    WHERE qfi.ts_code = fi.symbol
      AND COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) <= fi.time
    ORDER BY COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) DESC
    LIMIT 1
) qf ON true;

COMMENT ON VIEW v_fundamental_combined IS '日度估值与季度F-Score合并视图,季度数据以公告日为准向前填充';
