-- Migration: 019_add_index_daily
-- Description: Add index_daily table and convert it to TimescaleDB hypertable

CREATE TABLE IF NOT EXISTS index_daily (
    ts_code VARCHAR(20) NOT NULL,                   -- 指数代码，如 000300.SH（沪深300）
    trade_date TIMESTAMPTZ NOT NULL,                -- 交易日期
    close DECIMAL(20,6),                            -- 收盘点位
    open DECIMAL(20,6),                             -- 开盘点位
    high DECIMAL(20,6),                             -- 最高点位
    low DECIMAL(20,6),                              -- 最低点位
    pre_close DECIMAL(20,6),                        -- 昨日收盘点
    change DECIMAL(20,6),                           -- 涨跌点
    pct_chg DECIMAL(16,6),                          -- 涨跌幅（%）
    vol DECIMAL(30,6),                              -- 成交量（手）
    amount DECIMAL(30,6),                           -- 成交额（千元）
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_daily_trade_date ON index_daily(trade_date);

COMMENT ON TABLE index_daily IS '指数日线行情数据表 - 存储项目支持指数的日线行情数据';
COMMENT ON COLUMN index_daily.ts_code IS '指数代码，如 000300.SH（沪深300）';
COMMENT ON COLUMN index_daily.trade_date IS '交易日期';
COMMENT ON COLUMN index_daily.close IS '收盘点位';
COMMENT ON COLUMN index_daily.open IS '开盘点位';
COMMENT ON COLUMN index_daily.high IS '最高点位';
COMMENT ON COLUMN index_daily.low IS '最低点位';
COMMENT ON COLUMN index_daily.pre_close IS '昨日收盘点';
COMMENT ON COLUMN index_daily.change IS '涨跌点';
COMMENT ON COLUMN index_daily.pct_chg IS '涨跌幅（%）';
COMMENT ON COLUMN index_daily.vol IS '成交量（手）';
COMMENT ON COLUMN index_daily.amount IS '成交额（千元）';

-- 转换为 TimescaleDB 超表（如果尚未转换）
SELECT create_hypertable(
    'index_daily',
    'trade_date',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

