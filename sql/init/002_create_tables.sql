-- 创建基础数据表

-- 资产基本信息表
CREATE TABLE IF NOT EXISTS asset_basic (
    symbol VARCHAR(20) PRIMARY KEY,           -- 股票代码，如 600519.SH
    name VARCHAR(100) NOT NULL,               -- 证券名称
    market VARCHAR(20),                       -- 市场代码：主板、科创板、创业板、北交所等
    -- exchange VARCHAR(20),                     -- 交易所代码
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
    time TIMESTAMPTZ NOT NULL,                -- 交易日期
    symbol VARCHAR(20) NOT NULL,              -- 股票代码
    trade_volume BIGINT,                      -- 交易量（手）
    turnover_rate DECIMAL(10,6),              -- 换手率
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
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)                -- 复合主键替代 UNIQUE 约束
);

COMMENT ON TABLE daily_basic IS '每日指标表 - 存储PE、PB、换手率等每日计算指标';

-- 中国GDP数据表
CREATE TABLE IF NOT EXISTS cn_gdp (
    time TIMESTAMPTZ NOT NULL,                    -- 季度末日期，如 2025-03-31 表示 2025Q1
    quarter VARCHAR(10) NOT NULL,                 -- 季度，如 2024Q1
    gdp DECIMAL(20,2),                            -- GDP累计值（亿元）
    gdp_yoy DECIMAL(10,4),                        -- 当季同比增速（%）
    pi DECIMAL(20,2),                             -- 第一产业累计值（亿元）
    pi_yoy DECIMAL(10,4),                         -- 第一产业同比增速（%）
    si DECIMAL(20,2),                             -- 第二产业累计值（亿元）
    si_yoy DECIMAL(10,4),                         -- 第二产业同比增速（%）
    ti DECIMAL(20,2),                             -- 第三产业累计值（亿元）
    ti_yoy DECIMAL(10,4),                         -- 第三产业同比增速（%）
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time)                            -- 使用时间作为主键，支持增量更新
);

CREATE INDEX IF NOT EXISTS idx_cn_gdp_quarter ON cn_gdp(quarter);

COMMENT ON TABLE cn_gdp IS '中国国民经济GDP数据表 - 存储季度GDP及三次产业数据';
COMMENT ON COLUMN cn_gdp.time IS '季度末日期，格式如2025-03-31表示2025年第一季度末';
COMMENT ON COLUMN cn_gdp.quarter IS '季度，格式如2024Q1表示2024年第一季度';
COMMENT ON COLUMN cn_gdp.gdp IS 'GDP累计值（亿元）';
COMMENT ON COLUMN cn_gdp.gdp_yoy IS '当季同比增速（%）';
COMMENT ON COLUMN cn_gdp.pi IS '第一产业累计值（亿元）';
COMMENT ON COLUMN cn_gdp.pi_yoy IS '第一产业同比增速（%）';
COMMENT ON COLUMN cn_gdp.si IS '第二产业累计值（亿元）';
COMMENT ON COLUMN cn_gdp.si_yoy IS '第二产业同比增速（%）';
COMMENT ON COLUMN cn_gdp.ti IS '第三产业累计值（亿元）';
COMMENT ON COLUMN cn_gdp.ti_yoy IS '第三产业同比增速（%）';

-- 中国PPI数据表
CREATE TABLE IF NOT EXISTS cn_ppi (
    time TIMESTAMPTZ NOT NULL,                    -- 月末日期，如 2024-01-31
    month VARCHAR(10) NOT NULL,                   -- 月份（YYYYMM格式）
    ppi_yoy DECIMAL(10,4),                        -- PPI：全部工业品：当月同比
    ppi_mp_yoy DECIMAL(10,4),                     -- PPI：生产资料：当月同比
    ppi_mp_qm_yoy DECIMAL(10,4),                  -- PPI：生产资料：采掘业：当月同比
    ppi_mp_rm_yoy DECIMAL(10,4),                  -- PPI：生产资料：原料业：当月同比
    ppi_mp_p_yoy DECIMAL(10,4),                   -- PPI：生产资料：加工业：当月同比
    ppi_cg_yoy DECIMAL(10,4),                     -- PPI：生活资料：当月同比
    ppi_cg_f_yoy DECIMAL(10,4),                   -- PPI：生活资料：食品类：当月同比
    ppi_cg_c_yoy DECIMAL(10,4),                   -- PPI：生活资料：衣着类：当月同比
    ppi_cg_adu_yoy DECIMAL(10,4),                 -- PPI：生活资料：一般日用品类：当月同比
    ppi_cg_dcg_yoy DECIMAL(10,4),                 -- PPI：生活资料：耐用消费品类：当月同比
    ppi_mom DECIMAL(10,4),                        -- PPI：全部工业品：环比
    ppi_mp_mom DECIMAL(10,4),                     -- PPI：生产资料：环比
    ppi_mp_qm_mom DECIMAL(10,4),                  -- PPI：生产资料：采掘业：环比
    ppi_mp_rm_mom DECIMAL(10,4),                  -- PPI：生产资料：原料业：环比
    ppi_mp_p_mom DECIMAL(10,4),                   -- PPI：生产资料：加工业：环比
    ppi_cg_mom DECIMAL(10,4),                     -- PPI：生活资料：环比
    ppi_cg_f_mom DECIMAL(10,4),                   -- PPI：生活资料：食品类：环比
    ppi_cg_c_mom DECIMAL(10,4),                   -- PPI：生活资料：衣着类：环比
    ppi_cg_adu_mom DECIMAL(10,4),                 -- PPI：生活资料：一般日用品类：环比
    ppi_cg_dcg_mom DECIMAL(10,4),                 -- PPI：生活资料：耐用消费品类：环比
    ppi_accu DECIMAL(10,4),                       -- PPI：全部工业品：累计同比
    ppi_mp_accu DECIMAL(10,4),                    -- PPI：生产资料：累计同比
    ppi_mp_qm_accu DECIMAL(10,4),                 -- PPI：生产资料：采掘业：累计同比
    ppi_mp_rm_accu DECIMAL(10,4),                 -- PPI：生产资料：原料业：累计同比
    ppi_mp_p_accu DECIMAL(10,4),                  -- PPI：生产资料：加工业：累计同比
    ppi_cg_accu DECIMAL(10,4),                    -- PPI：生活资料：累计同比
    ppi_cg_f_accu DECIMAL(10,4),                  -- PPI：生活资料：食品类：累计同比
    ppi_cg_c_accu DECIMAL(10,4),                  -- PPI：生活资料：衣着类：累计同比
    ppi_cg_adu_accu DECIMAL(10,4),                -- PPI：生活资料：一般日用品类：累计同比
    ppi_cg_dcg_accu DECIMAL(10,4),                -- PPI：生活资料：耐用消费品类：累计同比
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time)
);

CREATE INDEX IF NOT EXISTS idx_cn_ppi_month ON cn_ppi(month);

COMMENT ON TABLE cn_ppi IS '中国PPI工业生产者出厂价格指数数据表';
COMMENT ON COLUMN cn_ppi.time IS '月份末日期，格式如2024-01-31表示2024年1月末';
COMMENT ON COLUMN cn_ppi.month IS '月份，格式如202401表示2024年1月';
COMMENT ON COLUMN cn_ppi.ppi_yoy IS 'PPI：全部工业品：当月同比（%）';
COMMENT ON COLUMN cn_ppi.ppi_mp_yoy IS 'PPI：生产资料：当月同比（%）';
COMMENT ON COLUMN cn_ppi.ppi_cg_yoy IS 'PPI：生活资料：当月同比（%）';
COMMENT ON COLUMN cn_ppi.ppi_mom IS 'PPI：全部工业品：环比（%）';
COMMENT ON COLUMN cn_ppi.ppi_accu IS 'PPI：全部工业品：累计同比（%）';
