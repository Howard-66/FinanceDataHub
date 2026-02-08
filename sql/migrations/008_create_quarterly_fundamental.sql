-- =====================================================
-- 008: 季度基本面指标表与日度估值表重构
-- 
-- 变更内容:
-- 1. 创建 quarterly_fundamental_indicators 季度表
-- 2. 修改 fundamental_indicators 表(移除F-Score字段,添加PEG)
-- 3. 创建 v_fundamental_combined 合并视图
-- =====================================================

-- =====================================================
-- 1. 创建季度基本面指标表
-- =====================================================
CREATE TABLE IF NOT EXISTS quarterly_fundamental_indicators (
    ts_code VARCHAR(20) NOT NULL,              -- 股票代码
    end_date_time TIMESTAMPTZ NOT NULL,        -- 报告期末
    ann_date_time TIMESTAMPTZ,                 -- 公告日期(fina_indicator使用)
    f_ann_date_time TIMESTAMPTZ,               -- 实际公告日(cashflow/balancesheet/income使用)
    
    -- Piotroski F-Score (9项)
    f_score SMALLINT,                          -- 总分 (0-9)
    f_roa SMALLINT,                            -- ROA > 0
    f_cfo SMALLINT,                            -- 经营现金流 > 0  
    f_delta_roa SMALLINT,                      -- ROA 同比增长
    f_accrual SMALLINT,                        -- 经营现金流 > 净利润
    f_delta_lever SMALLINT,                    -- 负债率下降
    f_delta_liquid SMALLINT,                   -- 流动比率上升
    f_eq_offer SMALLINT,                       -- 未增发股份
    f_delta_margin SMALLINT,                   -- 毛利率上升
    f_delta_turn SMALLINT,                     -- 周转率上升
    
    -- 补充基本面指标
    roe_5y_avg DECIMAL(10,4),                  -- 5年平均ROE
    ni_cfo_corr_3y DECIMAL(10,4),              -- 3年净利润-经营现金流相关性
    debt_ratio DECIMAL(10,4),                  -- 资产负债率(%)
    current_ratio DECIMAL(10,4),               -- 流动比率
    
    -- 行业豁免信息
    exemptions JSONB,                          -- 豁免规则列表
    
    -- 元数据
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ts_code, end_date_time)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_ts_code 
    ON quarterly_fundamental_indicators (ts_code);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_ann_date 
    ON quarterly_fundamental_indicators (ann_date_time);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_f_ann_date 
    ON quarterly_fundamental_indicators (f_ann_date_time);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_fscore 
    ON quarterly_fundamental_indicators (f_score);

-- 表和列注释
COMMENT ON TABLE quarterly_fundamental_indicators IS '季度基本面指标表(F-Score、ROE均值、相关性等)';
COMMENT ON COLUMN quarterly_fundamental_indicators.ts_code IS '股票代码,如600519.SH';
COMMENT ON COLUMN quarterly_fundamental_indicators.end_date_time IS '报告期末日期';
COMMENT ON COLUMN quarterly_fundamental_indicators.ann_date_time IS '公告日期(来自fina_indicator)';
COMMENT ON COLUMN quarterly_fundamental_indicators.f_ann_date_time IS '实际公告日期(来自cashflow/balancesheet/income,用于forward-fill基准)';
COMMENT ON COLUMN quarterly_fundamental_indicators.f_score IS 'Piotroski F-Score财务质量评分,0-9分';
COMMENT ON COLUMN quarterly_fundamental_indicators.roe_5y_avg IS '最近5年(20个季度)ROE平均值';
COMMENT ON COLUMN quarterly_fundamental_indicators.ni_cfo_corr_3y IS '最近3年(12个季度)净利润与经营现金流的相关系数';
COMMENT ON COLUMN quarterly_fundamental_indicators.debt_ratio IS '资产负债率=总负债/总资产*100';
COMMENT ON COLUMN quarterly_fundamental_indicators.current_ratio IS '流动比率=流动资产/流动负债';
COMMENT ON COLUMN quarterly_fundamental_indicators.exemptions IS '行业豁免规则JSON数组,如["f_score_cfo_positive","f_score_leverage"]';


-- =====================================================
-- 2. 修改 fundamental_indicators 表
-- =====================================================

-- 移除 F-Score 相关字段
ALTER TABLE fundamental_indicators 
    DROP COLUMN IF EXISTS f_score,
    DROP COLUMN IF EXISTS f_roa,
    DROP COLUMN IF EXISTS f_cfo,
    DROP COLUMN IF EXISTS f_delta_roa,
    DROP COLUMN IF EXISTS f_accrual,
    DROP COLUMN IF EXISTS f_delta_lever,
    DROP COLUMN IF EXISTS f_delta_liquid,
    DROP COLUMN IF EXISTS f_eq_offer,
    DROP COLUMN IF EXISTS f_delta_margin,
    DROP COLUMN IF EXISTS f_delta_turn;

-- 添加 PEG 字段
ALTER TABLE fundamental_indicators 
    ADD COLUMN IF NOT EXISTS peg DECIMAL(10,4);

COMMENT ON COLUMN fundamental_indicators.peg IS 'PEG = PE_TTM / 净利润增速(%), 仅当增速>0时有效';


-- =====================================================
-- 3. 创建合并视图
-- 使用 LATERAL JOIN 实现向前填充
-- 以 COALESCE(f_ann_date_time, ann_date_time) 为准
-- =====================================================
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
    qf.roe_5y_avg,
    qf.ni_cfo_corr_3y,
    qf.debt_ratio,
    qf.current_ratio,
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
