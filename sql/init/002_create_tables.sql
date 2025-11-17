-- 创建基础数据表

-- 资产基本信息表
CREATE TABLE IF NOT EXISTS asset_basic (
    symbol VARCHAR(20) PRIMARY KEY,           -- 股票代码，如 600519.SH
    name VARCHAR(100) NOT NULL,               -- 证券名称
    market VARCHAR(20),                       -- 市场代码：主板、科创板、创业板、北交所等
    area VARCHAR(50),                         -- 地区
    industry VARCHAR(50),                     -- 行业
    list_status VARCHAR(10) NOT NULL,         -- 上市状态：L-上市，D-退市，P-暂停上市
    list_date DATE,                           -- 上市日期
    delist_date DATE,                         -- 退市日期
    is_hs VARCHAR(5),                         -- 是否沪深港通标的：N-否，H-沪股通，S-深股通
    updated_at TIMESTAMPTZ DEFAULT NOW(),     -- 最后更新时间
    created_at TIMESTAMPTZ DEFAULT NOW()      -- 创建时间
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_asset_basic_market ON asset_basic(market);
CREATE INDEX IF NOT EXISTS idx_asset_basic_list_status ON asset_basic(list_status);
CREATE INDEX IF NOT EXISTS idx_asset_basic_industry ON asset_basic(industry);
CREATE INDEX IF NOT EXISTS idx_asset_basic_list_date ON asset_basic(list_date);

COMMENT ON TABLE asset_basic IS '资产基本信息表 - 存储股票、基金等资产的基础信息';
COMMENT ON COLUMN asset_basic.symbol IS '证券代码，如600519.SH';
COMMENT ON COLUMN asset_basic.market IS '市场分类：主板、科创板、创业板、北交所等';
COMMENT ON COLUMN asset_basic.list_status IS '上市状态：L-上市，D-退市，P-暂停上市';
COMMENT ON COLUMN asset_basic.is_hs IS '沪深港通：N-否，H-沪股通，S-深股通';

-- 每日指标表
CREATE TABLE IF NOT EXISTS daily_basic (
    id SERIAL PRIMARY KEY,
    time TIMESTAMPTZ NOT NULL,                -- 交易日期
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    trade_volume BIGINT,                      -- 交易量（手）
    turnover_rate DECIMAL(10,6),              -- 换手率
    turnover_rate_f DECIMAL(10,6),            -- 换手率（浮动）
    volume_ratio DECIMAL(10,6),               -- 量比
    pe DECIMAL(20,6),                         -- 市盈率
    pe_ttm DECIMAL(20,6),                     -- 市盈率TTM
    pb DECIMAL(20,6),                         -- 市净率
    ps DECIMAL(20,6),                         -- 市销率
    ps_ttm DECIMAL(20,6),                     -- 市销率TTM
    dv_ratio DECIMAL(20,6),                   -- 股息率
    dv_ttm DECIMAL(20,6),                     -- 股息率TTM
    total_share DECIMAL(20,6),                -- 总股本（万股）
    float_share DECIMAL(20,6),                -- 流通股本（万股）
    free_share DECIMAL(20,6),                 -- 限售股本（万股）
    total_mv DECIMAL(20,6),                   -- 总市值（万元）
    circ_mv DECIMAL(20,6),                    -- 流通市值（万元）
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_daily_basic_time ON daily_basic(time);
CREATE INDEX IF NOT EXISTS idx_daily_basic_symbol ON daily_basic(symbol);
CREATE INDEX IF NOT EXISTS idx_daily_basic_symbol_time ON daily_basic(symbol, time DESC);

COMMENT ON TABLE daily_basic IS '每日指标表 - 存储PE、PB、换手率等每日计算指标';
