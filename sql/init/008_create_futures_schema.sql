-- 创建期货专用 schema 和表

CREATE SCHEMA IF NOT EXISTS futures;

-- 期货合约基础信息
CREATE TABLE IF NOT EXISTS futures.contract_basic (
    symbol VARCHAR(32) PRIMARY KEY,              -- Tushare 合约代码，如 RB2405.SHF、RB.SHF、RBL.SHF
    product_code VARCHAR(16),                    -- 品种代码，如 RB
    exchange VARCHAR(16),                        -- 规范交易所代码，如 SHFE
    name VARCHAR(100),
    fut_code VARCHAR(32),
    contract_type VARCHAR(20),                   -- normal/main/continuous
    fut_type VARCHAR(20),
    multiplier DECIMAL(20,6),
    trade_unit VARCHAR(64),
    per_unit DECIMAL(20,6),
    quote_unit VARCHAR(64),
    quote_unit_desc TEXT,
    quote_unit_value DECIMAL(20,8),
    d_mode_desc TEXT,
    list_date DATE,
    delist_date DATE,
    d_month VARCHAR(32),
    last_ddate DATE,
    trade_time_desc TEXT,
    source VARCHAR(32) DEFAULT 'tushare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_futures_contract_basic_product ON futures.contract_basic(product_code);
CREATE INDEX IF NOT EXISTS idx_futures_contract_basic_exchange ON futures.contract_basic(exchange);
CREATE INDEX IF NOT EXISTS idx_futures_contract_basic_type ON futures.contract_basic(contract_type);
CREATE INDEX IF NOT EXISTS idx_futures_contract_basic_dates ON futures.contract_basic(list_date, delist_date);

COMMENT ON TABLE futures.contract_basic IS '期货合约基础信息表';

-- 期货主力/连续合约映射
CREATE TABLE IF NOT EXISTS futures.contract_mapping (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,                 -- 主力/连续合约代码，如 RB.SHF、RBL.SHF
    mapping_symbol VARCHAR(32) NOT NULL,         -- 映射到的实际合约代码，如 RB2405.SHF
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    contract_type VARCHAR(20),
    source VARCHAR(32) DEFAULT 'tushare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, mapping_symbol, time)
);

SELECT create_hypertable(
    'futures.contract_mapping',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

CREATE INDEX IF NOT EXISTS idx_futures_contract_mapping_product_time ON futures.contract_mapping(product_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_contract_mapping_type_time ON futures.contract_mapping(contract_type, time DESC);

-- 期货日线行情
CREATE TABLE IF NOT EXISTS futures.daily (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    pre_close DECIMAL(20,6),
    settle DECIMAL(20,6),
    pre_settle DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(30,6),
    open_interest DECIMAL(30,6),
    open_interest_chg DECIMAL(30,6),
    change1 DECIMAL(20,6),
    change2 DECIMAL(20,6),
    pct_chg DECIMAL(16,6),
    source VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.daily',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

CREATE INDEX IF NOT EXISTS idx_futures_daily_product_time ON futures.daily(product_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_daily_exchange_time ON futures.daily(exchange, time DESC);

-- 期货分钟线行情
CREATE TABLE IF NOT EXISTS futures.minute (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    frequency VARCHAR(8) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(30,6),
    open_interest DECIMAL(30,6),
    source VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time, frequency)
);

SELECT create_hypertable(
    'futures.minute',
    'time',
    partitioning_column => 'frequency',
    number_partitions => 5,
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 week'
);

CREATE INDEX IF NOT EXISTS idx_futures_minute_freq_symbol_time ON futures.minute(frequency, symbol, time DESC);

-- 期货结算参数
CREATE TABLE IF NOT EXISTS futures.settle (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    settle DECIMAL(20,6),
    trading_fee_rate DECIMAL(20,10),
    trading_fee DECIMAL(20,6),
    delivery_fee DECIMAL(20,6),
    b_hedging_margin_rate DECIMAL(20,10),
    s_hedging_margin_rate DECIMAL(20,10),
    long_margin_rate DECIMAL(20,10),
    short_margin_rate DECIMAL(20,10),
    offset_today_fee DECIMAL(20,6),
    source VARCHAR(32) DEFAULT 'tushare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.settle',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

CREATE INDEX IF NOT EXISTS idx_futures_settle_product_time ON futures.settle(product_code, time DESC);

-- 南华等期货指数日线行情
CREATE TABLE IF NOT EXISTS futures.index_daily (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    pre_close DECIMAL(20,6),
    change DECIMAL(20,6),
    pct_chg DECIMAL(16,6),
    volume DECIMAL(30,6),
    amount DECIMAL(30,6),
    source VARCHAR(32) DEFAULT 'tushare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.index_daily',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

-- 现货价格与基差
CREATE TABLE IF NOT EXISTS futures.spot_basis (
    time TIMESTAMPTZ NOT NULL,
    product_code VARCHAR(16) NOT NULL,
    exchange VARCHAR(16),
    spot_price DECIMAL(20,6),
    futures_price DECIMAL(20,6),
    near_contract VARCHAR(32),
    near_contract_price DECIMAL(20,6),
    dominant_contract VARCHAR(32),
    dominant_contract_price DECIMAL(20,6),
    near_basis DECIMAL(20,6),
    near_basis_rate DECIMAL(20,10),
    dom_basis DECIMAL(20,6),
    dom_basis_rate DECIMAL(20,10),
    source VARCHAR(32) DEFAULT 'akshare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_code, time)
);

SELECT create_hypertable(
    'futures.spot_basis',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

-- 库存
CREATE TABLE IF NOT EXISTS futures.inventory_receipt (
    time TIMESTAMPTZ NOT NULL,
    product_code VARCHAR(16) NOT NULL,
    inventory DECIMAL(30,6),
    source VARCHAR(32) DEFAULT 'akshare',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_code, time)
);

SELECT create_hypertable(
    'futures.inventory_receipt',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

-- 期限结构快照与派生指标
CREATE TABLE IF NOT EXISTS futures.term_metrics (
    time TIMESTAMPTZ NOT NULL,
    product_code VARCHAR(16) NOT NULL,
    exchange VARCHAR(16),
    flag DECIMAL(10,4),
    primary_contract VARCHAR(32),
    primary_contract_close DECIMAL(20,6),
    secondary_contract VARCHAR(32),
    secondary_contract_close DECIMAL(20,6),
    spread DECIMAL(20,6),
    days_to_primary_expiry INTEGER,
    days_between_expiry INTEGER,
    annualized_roll_yield DECIMAL(20,10),
    candidate_count INTEGER,
    source VARCHAR(32) DEFAULT 'preprocess',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_code, time)
);

SELECT create_hypertable(
    'futures.term_metrics',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);
