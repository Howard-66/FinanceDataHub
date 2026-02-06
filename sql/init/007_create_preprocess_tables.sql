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
CREATE TABLE IF NOT EXISTS fundamental_indicators (
    time TIMESTAMPTZ NOT NULL,              -- 数据日期
    symbol VARCHAR(20) NOT NULL,            -- 股票代码
    
    -- 估值分位数（5年滚动窗口）
    pe_ttm_pct_1250d DECIMAL(10,4),         -- PE_TTM 5年分位
    pb_pct_1250d DECIMAL(10,4),             -- PB 5年分位
    ps_ttm_pct_1250d DECIMAL(10,4),         -- PS_TTM 5年分位
    
    -- 估值分位数（10年滚动窗口）
    pe_ttm_pct_2500d DECIMAL(10,4),         -- PE_TTM 10年分位
    pb_pct_2500d DECIMAL(10,4),             -- PB 10年分位
    ps_ttm_pct_2500d DECIMAL(10,4),         -- PS_TTM 10年分位
    
    -- Piotroski F-Score
    f_score SMALLINT,                       -- 总分 (0-9)
    f_roa SMALLINT,                         -- ROA > 0
    f_cfo SMALLINT,                         -- 经营现金流 > 0
    f_delta_roa SMALLINT,                   -- ROA 同比增长
    f_accrual SMALLINT,                     -- 经营现金流 > 净利润
    f_delta_lever SMALLINT,                 -- 负债率下降
    f_delta_liquid SMALLINT,                -- 流动比率上升
    f_eq_offer SMALLINT,                    -- 未增发股份
    f_delta_margin SMALLINT,                -- 毛利率上升
    f_delta_turn SMALLINT,                  -- 周转率上升
    
    -- 元数据
    processed_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (symbol, time)
);

-- 转换为超表
SELECT create_hypertable('fundamental_indicators', 'time', 
    if_not_exists => TRUE, 
    create_default_indexes => FALSE,
    chunk_time_interval => INTERVAL '1 month'
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_fundamental_indicators_symbol 
    ON fundamental_indicators (symbol);
CREATE INDEX IF NOT EXISTS idx_fundamental_indicators_time 
    ON fundamental_indicators (time DESC);
CREATE INDEX IF NOT EXISTS idx_fundamental_indicators_fscore 
    ON fundamental_indicators (f_score);

-- 表和列注释
COMMENT ON TABLE fundamental_indicators IS '基本面指标（估值分位、F-Score）';
COMMENT ON COLUMN fundamental_indicators.pe_ttm_pct_250d IS 'PE_TTM在过去1年（250交易日）的分位数，0-100';
COMMENT ON COLUMN fundamental_indicators.f_score IS 'Piotroski F-Score财务质量评分，0-9分';
COMMENT ON COLUMN fundamental_indicators.f_roa IS 'F-Score子项：ROA是否为正（0或1）';


-- =====================================================
-- 5. 预处理执行日志表
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
