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
    change_pct DECIMAL(16,6),                 -- 涨跌幅（扩展精度以容纳1990年代极端值如18430%）
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
    frequency VARCHAR(5) NOT NULL,            -- 数据频率：1m, 5m, 15m, 30m, 60m
    open DECIMAL(20,6),                       -- 开盘价
    high DECIMAL(20,6),                       -- 最高价
    low DECIMAL(20,6),                        -- 最低价
    close DECIMAL(20,6),                      -- 收盘价
    volume BIGINT,                            -- 成交量
    amount DECIMAL(30,6),                     -- 成交额
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time, frequency)     -- 三列复合主键，确保同股票、同时间、不同频率可共存
);

-- 添加频率索引以优化按频率查询
CREATE INDEX IF NOT EXISTS idx_symbol_minute_freq ON symbol_minute(frequency, symbol, time DESC);

-- ⚠️  删除冗余索引：PRIMARY KEY 已自动创建 (symbol, time, frequency) 索引
-- ⚠️  删除冗余索引：create_hypertable 会自动在 time 列创建索引

-- 将symbol_minute转换为TimescaleDB超表，使用复合分区
-- 时间分区：1周（主分区）+ 频率分区：5个桶（二级分区）
SELECT create_hypertable(
    'symbol_minute',
    'time',
    partitioning_column => 'frequency',       -- 按频率进行二级分区
    number_partitions => 5,                   -- 5个频率分区（1m, 5m, 15m, 30m, 60m）
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 week'  -- 时间分区间隔
);

COMMENT ON TABLE symbol_minute IS '分钟数据表 - 存储股票分钟级K线数据（TimescaleDB超表，按时间和频率复合分区）';
COMMENT ON COLUMN symbol_minute.frequency IS '数据频率：1m-1分钟, 5m-5分钟, 15m-15分钟, 30m-30分钟, 60m-60分钟';

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
    adj_factor DECIMAL(20,10) NOT NULL,       -- 复权因子（Tushare统一复权因子，用于前复权/后复权计算）
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

-- 指数日线行情数据表超表
-- 将index_daily转换为TimescaleDB超表，设置分区间隔为5年
SELECT create_hypertable(
    'index_daily',
    'trade_date',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

COMMENT ON TABLE index_daily IS '指数日线行情数据表 - 存储项目支持指数的日线行情数据（TimescaleDB超表）';

-- 申万行业日线行情数据表超表
-- 将sw_daily转换为TimescaleDB超表，设置分区间隔为5年
SELECT create_hypertable(
    'sw_daily',
    'trade_date',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

-- 为行业代码添加索引（超表不会自动为 ts_code 创建索引）
CREATE INDEX IF NOT EXISTS idx_sw_daily_ts_code ON sw_daily(ts_code);

COMMENT ON TABLE sw_daily IS '申万行业日线行情数据表 - 存储申万行业指数的日线行情数据（TimescaleDB超表）';
