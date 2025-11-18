-- 创建TimescaleDB超表

-- 日线数据表
CREATE TABLE IF NOT EXISTS symbol_daily (
    time TIMESTAMPTZ NOT NULL,                -- 交易时间
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    open DECIMAL(20,6),                       -- 开盘价
    high DECIMAL(20,6),                       -- 最高价
    low DECIMAL(20,6),                        -- 最低价
    close DECIMAL(20,6),                      -- 收盘价
    volume BIGINT,                            -- 成交量
    amount DECIMAL(30,6),                     -- 成交额
    adj_factor DECIMAL(20,10),                -- 复权因子
    open_interest BIGINT,                     -- 持仓量（期货）
    settle DECIMAL(20,6),                     -- 结算价（期货）
    change_pct DECIMAL(10,6),                 -- 涨跌幅
    change_amount DECIMAL(20,6),              -- 涨跌额
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

-- ⚠️  删除冗余索引：PRIMARY KEY 已自动创建 (symbol, time) 索引
-- ⚠️  删除冗余索引：create_hypertable 会自动在 time 列创建索引
-- ⚠️  删除高基数值索引：idx_symbol_daily_close 对写入性能有害

-- 将symbol_daily转换为TimescaleDB超表，设置分区间隔为5年
SELECT create_hypertable(
    'symbol_daily',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

COMMENT ON TABLE symbol_daily IS '日线数据表 - 存储股票日K线数据（TimescaleDB超表）';

-- 分钟数据表
CREATE TABLE IF NOT EXISTS symbol_minute (
    time TIMESTAMPTZ NOT NULL,                -- 交易时间
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    open DECIMAL(20,6),                       -- 开盘价
    high DECIMAL(20,6),                       -- 最高价
    low DECIMAL(20,6),                        -- 最低价
    close DECIMAL(20,6),                      -- 收盘价
    volume BIGINT,                            -- 成交量
    amount DECIMAL(30,6),                     -- 成交额
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

-- ⚠️  删除冗余索引：PRIMARY KEY 已自动创建 (symbol, time) 索引
-- ⚠️  删除冗余索引：create_hypertable 会自动在 time 列创建索引

-- 将symbol_minute转换为TimescaleDB超表，设置分区间隔为1周
SELECT create_hypertable(
    'symbol_minute',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 week'
);

COMMENT ON TABLE symbol_minute IS '分钟数据表 - 存储股票分钟级K线数据（TimescaleDB超表）';

-- Tick级别数据表（实时成交）
CREATE TABLE IF NOT EXISTS symbol_tick (
    time TIMESTAMPTZ NOT NULL,                -- 交易时间
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    price DECIMAL(20,6) NOT NULL,             -- 成交价
    volume BIGINT NOT NULL,                   -- 成交量
    amount DECIMAL(30,6),                     -- 成交额
    side VARCHAR(10),                         -- 买卖方向：B-买入，S-卖出
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)                -- 添加复合主键，确保同股票同时间唯一性
);

-- ⚠️  删除冗余索引：PRIMARY KEY 已自动创建 (symbol, time) 索引
-- ⚠️  删除冗余索引：create_hypertable 会自动在 time 列创建索引
-- ⚠️  删除高基数值索引：idx_symbol_tick_price 对写入性能有害

-- 将symbol_tick转换为TimescaleDB超表，设置分区间隔为1天
SELECT create_hypertable(
    'symbol_tick',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 day'
);

COMMENT ON TABLE symbol_tick IS 'Tick数据表 - 存储逐笔交易数据（TimescaleDB超表）';

-- 复权因子表
CREATE TABLE IF NOT EXISTS adj_factor (
    time TIMESTAMPTZ NOT NULL,                -- 日期
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    adj_factor DECIMAL(20,10) NOT NULL,       -- 复权因子
    adj_type VARCHAR(10),                     -- 复权类型：qfq-前复权，hfq-后复权
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

-- ⚠️  删除冗余索引：PRIMARY KEY 已自动创建 (symbol, time) 索引
-- ⚠️  删除冗余索引：create_hypertable 会自动在 time 列创建索引

-- 将adj_factor转换为TimescaleDB超表，设置分区间隔为5年
SELECT create_hypertable(
    'adj_factor',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

-- 每日指标表（从 002 中移入）
-- 将daily_basic转换为TimescaleDB超表，设置分区间隔为5年
SELECT create_hypertable(
    'daily_basic',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

COMMENT ON TABLE adj_factor IS '复权因子表 - 存储股票复权因子数据（TimescaleDB超表）';
