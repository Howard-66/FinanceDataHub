-- ==========================================
-- 迁移: 添加 TTM 指标字段到季度基本面指标表
-- 新增字段: cfo_ttm, ni_ttm, gpm_ttm, at_ttm
-- 用于存储滚动 4 期聚合的财务指标 TTM 值
-- ==========================================

-- 添加 TTM 指标字段
ALTER TABLE quarterly_fundamental_indicators
    ADD COLUMN IF NOT EXISTS cfo_ttm DECIMAL(20,4);

ALTER TABLE quarterly_fundamental_indicators
    ADD COLUMN IF NOT EXISTS ni_ttm DECIMAL(20,4);

ALTER TABLE quarterly_fundamental_indicators
    ADD COLUMN IF NOT EXISTS gpm_ttm DECIMAL(10,4);

ALTER TABLE quarterly_fundamental_indicators
    ADD COLUMN IF NOT EXISTS at_ttm DECIMAL(10,4);

-- 添加字段注释
COMMENT ON COLUMN quarterly_fundamental_indicators.cfo_ttm
    IS '经营现金流 TTM，4期滚动求和';

COMMENT ON COLUMN quarterly_fundamental_indicators.ni_ttm
    IS '净利润 TTM，4期滚动求和';

COMMENT ON COLUMN quarterly_fundamental_indicators.gpm_ttm
    IS '毛利率 TTM，q_gsprofit_margin 4期滚动均值';

COMMENT ON COLUMN quarterly_fundamental_indicators.at_ttm
    IS '资产周转率 TTM，4期滚动均值';

-- 更新合并视图，添加 TTM 字段
CREATE OR REPLACE VIEW v_fundamental_combined AS
SELECT
    fi.time,
    fi.symbol,
    -- 日度估值指标
    fi.pe_ttm,
    fi.pb,
    fi.ps_ttm,
    fi.peg,
    fi.pe_ttm_pct_1250d,
    fi.pb_pct_1250d,
    fi.ps_ttm_pct_1250d,
    fi.pe_ttm_pct_2500d,
    fi.pb_pct_2500d,
    fi.ps_ttm_pct_2500d,
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
FROM fundamental_indicators fi
LEFT JOIN LATERAL (
    SELECT * FROM quarterly_fundamental_indicators qfi
    WHERE qfi.ts_code = fi.symbol
      AND COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) <= fi.time
    ORDER BY COALESCE(qfi.f_ann_date_time, qfi.ann_date_time) DESC
    LIMIT 1
) qf ON true;

COMMENT ON VIEW v_fundamental_combined IS '日度估值与季度F-Score合并视图,季度数据以公告日为准向前填充';
