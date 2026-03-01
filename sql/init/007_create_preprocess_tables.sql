-- =====================================================
-- 预处理数据表
-- 存储经过复权处理和技术指标计算的数据
-- =====================================================

-- 启用 TimescaleDB（如果还没有启用）
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- =====================================================
-- 1. 前复权日线数据表（含技术指标）
-- =====================================================
CREATE TABLE IF NOT EXISTS processed_daily_qfq (
    time TIMESTAMPTZ NOT NULL,              -- 交易日期
    symbol VARCHAR(20) NOT NULL,            -- 股票代码
    
    -- OHLCV 数据（前复权后）
    open DECIMAL(20,6),                     -- 开盘价
    high DECIMAL(20,6),                     -- 最高价
    low DECIMAL(20,6),                      -- 最低价
    close DECIMAL(20,6),                    -- 收盘价
    volume BIGINT,                          -- 成交量（股）
    amount DECIMAL(20,4),                   -- 成交额（元）
    
    -- 均线指标 (简化版: 20, 50)
    ma_20 DECIMAL(20,6),                    -- 20日均线
    ma_50 DECIMAL(20,6),                    -- 50日均线
    
    -- MACD 指标 (默认 12, 26, 9)
    macd_dif DECIMAL(20,6),                 -- DIF 线
    macd_dea DECIMAL(20,6),                 -- DEA 信号线
    macd_hist DECIMAL(20,6),                -- MACD 柱状图
    
    -- RSI 指标 (简化版: 14)
    rsi_14 DECIMAL(10,4),                   -- 14日 RSI
    
    -- ATR 指标 (简化版: 14)
    atr_14 DECIMAL(20,6),                   -- 14日 ATR

    -- 元数据
    last_adj_factor DECIMAL(20,10),         -- 最新复权因子(用于智能增量检测)
    processed_at TIMESTAMPTZ DEFAULT NOW(), -- 处理时间

    PRIMARY KEY (symbol, time)
);

-- 转换为超表
SELECT create_hypertable('processed_daily_qfq', 'time', 
    if_not_exists => TRUE, 
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 month'
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_processed_daily_qfq_symbol 
    ON processed_daily_qfq (symbol);
CREATE INDEX IF NOT EXISTS idx_processed_daily_qfq_time 
    ON processed_daily_qfq (time DESC);

-- 表和列注释
COMMENT ON TABLE processed_daily_qfq IS '前复权日线数据（含技术指标）';
COMMENT ON COLUMN processed_daily_qfq.ma_20 IS '20日简单移动平均线';
COMMENT ON COLUMN processed_daily_qfq.macd_dif IS 'MACD DIF线（快线-慢线）';
COMMENT ON COLUMN processed_daily_qfq.macd_dea IS 'MACD DEA信号线（DIF的EMA）';
COMMENT ON COLUMN processed_daily_qfq.macd_hist IS 'MACD柱状图（DIF-DEA)*2';
COMMENT ON COLUMN processed_daily_qfq.rsi_14 IS '14日RSI相对强弱指标（0-100）';
COMMENT ON COLUMN processed_daily_qfq.atr_14 IS '14日平均真实波幅';


-- =====================================================
-- 2. 前复权周线数据表（含技术指标）
-- =====================================================
CREATE TABLE IF NOT EXISTS processed_weekly_qfq (
    time TIMESTAMPTZ NOT NULL,              -- 周末日期
    symbol VARCHAR(20) NOT NULL,            -- 股票代码
    
    -- OHLCV 数据（前复权后）
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(20,4),
    
    -- 均线指标（基于周线）
    ma_20 DECIMAL(20,6),                    -- 20周均线
    ma_50 DECIMAL(20,6),                    -- 50周均线
    
    -- MACD 指标
    macd_dif DECIMAL(20,6),
    macd_dea DECIMAL(20,6),
    macd_hist DECIMAL(20,6),
    
    -- RSI 指标
    rsi_14 DECIMAL(10,4),
    
    -- ATR 指标
    atr_14 DECIMAL(20,6),

    -- 元数据
    last_adj_factor DECIMAL(20,10),         -- 最新复权因子(用于智能增量检测)
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, time)
);

-- 转换为超表
SELECT create_hypertable('processed_weekly_qfq', 'time', 
    if_not_exists => TRUE, 
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '3 months'
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_processed_weekly_qfq_symbol 
    ON processed_weekly_qfq (symbol);
CREATE INDEX IF NOT EXISTS idx_processed_weekly_qfq_time 
    ON processed_weekly_qfq (time DESC);

COMMENT ON TABLE processed_weekly_qfq IS '前复权周线数据（含技术指标）';


-- =====================================================
-- 3. 前复权月线数据表（含技术指标）
-- =====================================================
CREATE TABLE IF NOT EXISTS processed_monthly_qfq (
    time TIMESTAMPTZ NOT NULL,              -- 月末日期
    symbol VARCHAR(20) NOT NULL,            -- 股票代码
    
    -- OHLCV 数据（前复权后）
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(20,4),
    
    -- 均线指标（基于月线）
    ma_20 DECIMAL(20,6),                    -- 20月均线
    ma_50 DECIMAL(20,6),                    -- 50月均线
    
    -- MACD 指标
    macd_dif DECIMAL(20,6),
    macd_dea DECIMAL(20,6),
    macd_hist DECIMAL(20,6),
    
    -- RSI 指标
    rsi_14 DECIMAL(10,4),
    
    -- ATR 指标
    atr_14 DECIMAL(20,6),

    -- 元数据
    last_adj_factor DECIMAL(20,10),         -- 最新复权因子(用于智能增量检测)
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, time)
);

-- 转换为超表
SELECT create_hypertable('processed_monthly_qfq', 'time', 
    if_not_exists => TRUE, 
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 year'
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_processed_monthly_qfq_symbol 
    ON processed_monthly_qfq (symbol);
CREATE INDEX IF NOT EXISTS idx_processed_monthly_qfq_time 
    ON processed_monthly_qfq (time DESC);

COMMENT ON TABLE processed_monthly_qfq IS '前复权月线数据（含技术指标）';


-- =====================================================
-- 4. 基本面指标表
-- =====================================================
CREATE TABLE IF NOT EXISTS processed_valuation_pct (
    time TIMESTAMPTZ NOT NULL,              -- 数据日期
    symbol VARCHAR(20) NOT NULL,            -- 股票代码

    -- 估值指标原始值
    pe_ttm DECIMAL(20,4),                   -- 市盈率(TTM)
    pb DECIMAL(20,4),                       -- 市净率
    ps_ttm DECIMAL(20,4),                   -- 市销率(TTM)
    dv_ttm DECIMAL(20,4),                  -- 股息率(TTM)
    peg DECIMAL(20,4),                      -- PEG = PE_TTM / 净利润增速(%)

    -- 估值分位数（5年滚动窗口）
    pe_ttm_pct_1250d DECIMAL(20,4),         -- PE_TTM 5年分位
    pb_pct_1250d DECIMAL(20,4),             -- PB 5年分位
    ps_ttm_pct_1250d DECIMAL(20,4),         -- PS_TTM 5年分位

    -- 估值分位数（10年滚动窗口）
    -- pe_ttm_pct_2500d DECIMAL(10,4),         -- PE_TTM 10年分位
    -- pb_pct_2500d DECIMAL(10,4),             -- PB 10年分位
    -- ps_ttm_pct_2500d DECIMAL(10,4),         -- PS_TTM 10年分位

    -- 元数据
    processed_at TIMESTAMPTZ DEFAULT NOW(),

    PRIMARY KEY (symbol, time)
);

-- 转换为超表
SELECT create_hypertable('processed_valuation_pct', 'time', 
    if_not_exists => TRUE, 
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 month'
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_processed_valuation_pct_symbol 
    ON processed_valuation_pct (symbol);
CREATE INDEX IF NOT EXISTS idx_processed_valuation_pct_time 
    ON processed_valuation_pct (time DESC);

-- 表和列注释
COMMENT ON TABLE processed_valuation_pct IS '日度估值指标（PE/PB/PS分位）';
COMMENT ON COLUMN processed_valuation_pct.pe_ttm IS '市盈率(TTM)';
COMMENT ON COLUMN processed_valuation_pct.dv_ttm IS '股息率(TTM)，税前股息率';
COMMENT ON COLUMN processed_valuation_pct.peg IS 'PEG = PE_TTM / 净利润增速(%), 仅当增速>0时有效';
COMMENT ON COLUMN processed_valuation_pct.pe_ttm_pct_1250d IS 'PE_TTM在过去5年（1250交易日）的分位数，0-100';
COMMENT ON COLUMN processed_valuation_pct.dv_ttm_pct_1250d IS '股息率 TTM 在过去5年（1250交易日）的分位数，0-100';


-- =====================================================
-- 5. 季度基本面指标表（F-Score等）
-- =====================================================
CREATE TABLE IF NOT EXISTS processed_fundamental_quality (
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
    roa_ttm DECIMAL(20,4),                     -- ROA TTM
    roe_5y_avg DECIMAL(20,4),                  -- 5年平均ROE
    ni_cfo_corr_3y DECIMAL(20,4),              -- 3年净利润-经营现金流相关性
    debt_ratio DECIMAL(20,4),                  -- 资产负债率(%)
    current_ratio DECIMAL(20,4),                -- 流动比率
    -- TTM 指标 (滚动4期聚合)
    cfo_ttm DECIMAL(20,4),                     -- 经营现金流 TTM (4期求和)
    ni_ttm DECIMAL(20,4),                      -- 净利润 TTM (4期求和)
    gpm_ttm DECIMAL(20,4),                     -- 毛利率 TTM (4期均值)
    at_ttm DECIMAL(20,4),                      -- 资产周转率 TTM (4期均值)
    
    -- 行业豁免信息
    exemptions JSONB,                          -- 豁免规则列表
    
    -- 元数据
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (ts_code, end_date_time)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_ts_code 
    ON processed_fundamental_quality (ts_code);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_ann_date 
    ON processed_fundamental_quality (ann_date_time);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_f_ann_date 
    ON processed_fundamental_quality (f_ann_date_time);
CREATE INDEX IF NOT EXISTS idx_quarterly_fund_fscore 
    ON processed_fundamental_quality (f_score);

-- 表和列注释
COMMENT ON TABLE processed_fundamental_quality IS '季度基本面指标表(F-Score、ROE均值、相关性、TTM指标等)';
COMMENT ON COLUMN processed_fundamental_quality.ts_code IS '股票代码,如600519.SH';
COMMENT ON COLUMN processed_fundamental_quality.end_date_time IS '报告期末日期';
COMMENT ON COLUMN processed_fundamental_quality.ann_date_time IS '公告日期(来自fina_indicator)';
COMMENT ON COLUMN processed_fundamental_quality.f_ann_date_time IS '实际公告日期(来自cashflow/balancesheet/income,用于forward-fill基准)';
COMMENT ON COLUMN processed_fundamental_quality.f_score IS 'Piotroski F-Score财务质量评分,0-9分';
COMMENT ON COLUMN processed_fundamental_quality.roe_5y_avg IS '最近5年(20个季度)ROE平均值';
COMMENT ON COLUMN processed_fundamental_quality.ni_cfo_corr_3y IS '最近3年(12个季度)净利润与经营现金流的相关系数';
COMMENT ON COLUMN processed_fundamental_quality.debt_ratio IS '资产负债率=总负债/总资产*100';
COMMENT ON COLUMN processed_fundamental_quality.current_ratio IS '流动比率=流动资产/流动负债';
COMMENT ON COLUMN processed_fundamental_quality.cfo_ttm IS '经营现金流 TTM，4期滚动求和';
COMMENT ON COLUMN processed_fundamental_quality.ni_ttm IS '净利润 TTM，4期滚动求和';
COMMENT ON COLUMN processed_fundamental_quality.gpm_ttm IS '毛利率 TTM，q_gsprofit_margin 4期滚动均值';
COMMENT ON COLUMN processed_fundamental_quality.at_ttm IS '资产周转率 TTM，4期滚动均值';
COMMENT ON COLUMN processed_fundamental_quality.exemptions IS '行业豁免规则JSON数组,如["f_score_cfo_positive","f_score_leverage"]';


-- =====================================================
-- 6. 日度估值与季度F-Score合并视图
-- =====================================================
DROP VIEW IF EXISTS v_fundamental_combined;
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
    fi.dv_ttm_pct_1250d,
    -- fi.pe_ttm_pct_2500d,
    -- fi.pb_pct_2500d,
    -- fi.ps_ttm_pct_2500d,
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


-- =====================================================
-- 7. 预处理执行日志表
-- =====================================================
CREATE TABLE IF NOT EXISTS preprocess_execution_log (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(100) NOT NULL,           -- 任务 ID
    job_type VARCHAR(50) NOT NULL,          -- 任务类型 (technical/fundamental)
    status VARCHAR(20) NOT NULL,            -- 状态 (running/completed/failed)
    started_at TIMESTAMPTZ NOT NULL,        -- 开始时间
    ended_at TIMESTAMPTZ,                   -- 结束时间
    symbols_count INTEGER,                  -- 处理股票数
    records_processed INTEGER,              -- 处理记录数
    error_message TEXT,                     -- 错误信息
    parameters JSONB,                       -- 任务参数
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_preprocess_log_job_id 
    ON preprocess_execution_log (job_id);
CREATE INDEX IF NOT EXISTS idx_preprocess_log_started_at 
    ON preprocess_execution_log (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_preprocess_log_status 
    ON preprocess_execution_log (status);

COMMENT ON TABLE preprocess_execution_log IS '预处理任务执行日志';
