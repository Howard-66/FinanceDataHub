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
    turnover_rate DECIMAL(20,6),              -- 换手率
    volume_ratio DECIMAL(20,6),               -- 量比
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

-- 中国货币供应量数据表
CREATE TABLE IF NOT EXISTS cn_m (
    time TIMESTAMPTZ NOT NULL,                    -- 月末日期，如 2024-01-31
    month VARCHAR(10) NOT NULL,                   -- 月份（YYYYMM格式）
    m0 DECIMAL(20,2),                             -- M0（亿元）
    m0_yoy DECIMAL(10,4),                         -- M0同比（%）
    m0_mom DECIMAL(10,4),                         -- M0环比（%）
    m1 DECIMAL(20,2),                             -- M1（亿元）
    m1_yoy DECIMAL(10,4),                         -- M1同比（%）
    m1_mom DECIMAL(10,4),                         -- M1环比（%）
    m2 DECIMAL(20,2),                             -- M2（亿元）
    m2_yoy DECIMAL(10,4),                         -- M2同比（%）
    m2_mom DECIMAL(10,4),                         -- M2环比（%）
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time)
);

CREATE INDEX IF NOT EXISTS idx_cn_m_month ON cn_m(month);

COMMENT ON TABLE cn_m IS '中国货币供应量数据表 - 存储M0、M1、M2月度数据';
COMMENT ON COLUMN cn_m.time IS '月份末日期，格式如2024-01-31表示2024年1月末';
COMMENT ON COLUMN cn_m.month IS '月份，格式如202401表示2024年1月';
COMMENT ON COLUMN cn_m.m0 IS 'M0货币供应量（亿元）';
COMMENT ON COLUMN cn_m.m0_yoy IS 'M0同比增速（%）';
COMMENT ON COLUMN cn_m.m0_mom IS 'M0环比增速（%）';
COMMENT ON COLUMN cn_m.m1 IS 'M1货币供应量（亿元）';
COMMENT ON COLUMN cn_m.m1_yoy IS 'M1同比增速（%）';
COMMENT ON COLUMN cn_m.m1_mom IS 'M1环比增速（%）';
COMMENT ON COLUMN cn_m.m2 IS 'M2货币供应量（亿元）';
COMMENT ON COLUMN cn_m.m2_yoy IS 'M2同比增速（%）';
COMMENT ON COLUMN cn_m.m2_mom IS 'M2环比增速（%）';

-- 中国PMI数据表
CREATE TABLE IF NOT EXISTS cn_pmi (
    time TIMESTAMPTZ NOT NULL,                    -- 月末日期，如 2024-01-31
    month VARCHAR(10) NOT NULL,                   -- 月份（YYYYMM格式）
    pmi010000 DECIMAL(10,4),                      -- 制造业PMI
    pmi010100 DECIMAL(10,4),                      -- 制造业PMI:企业规模/大型企业
    pmi010200 DECIMAL(10,4),                      -- 制造业PMI:企业规模/中型企业
    pmi010300 DECIMAL(10,4),                      -- 制造业PMI:企业规模/小型企业
    pmi010400 DECIMAL(10,4),                      -- 制造业PMI:构成指数/生产指数
    pmi010401 DECIMAL(10,4),                      -- 制造业PMI:生产指数:大型企业
    pmi010402 DECIMAL(10,4),                      -- 制造业PMI:生产指数:中型企业
    pmi010403 DECIMAL(10,4),                      -- 制造业PMI:生产指数:小型企业
    pmi010500 DECIMAL(10,4),                      -- 制造业PMI:新订单指数
    pmi010501 DECIMAL(10,4),                      -- 制造业PMI:新订单指数:大型企业
    pmi010502 DECIMAL(10,4),                      -- 制造业PMI:新订单指数:中型企业
    pmi010503 DECIMAL(10,4),                      -- 制造业PMI:新订单指数:小型企业
    pmi010600 DECIMAL(10,4),                      -- 制造业PMI:供应商配送时间指数
    pmi010601 DECIMAL(10,4),                      -- 制造业PMI:供应商配送时间指数:大型企业
    pmi010602 DECIMAL(10,4),                      -- 制造业PMI:供应商配送时间指数:中型企业
    pmi010603 DECIMAL(10,4),                      -- 制造业PMI:供应商配送时间指数:小型企业
    pmi010700 DECIMAL(10,4),                      -- 制造业PMI:原材料库存指数
    pmi010701 DECIMAL(10,4),                      -- 制造业PMI:原材料库存指数:大型企业
    pmi010702 DECIMAL(10,4),                      -- 制造业PMI:原材料库存指数:中型企业
    pmi010703 DECIMAL(10,4),                      -- 制造业PMI:原材料库存指数:小型企业
    pmi010800 DECIMAL(10,4),                      -- 制造业PMI:从业人员指数
    pmi010801 DECIMAL(10,4),                      -- 制造业PMI:从业人员指数:大型企业
    pmi010802 DECIMAL(10,4),                      -- 制造业PMI:从业人员指数:中型企业
    pmi010803 DECIMAL(10,4),                      -- 制造业PMI:从业人员指数:小型企业
    pmi010900 DECIMAL(10,4),                      -- 制造业PMI:新出口订单
    pmi011000 DECIMAL(10,4),                      -- 制造业PMI:进口
    pmi011100 DECIMAL(10,4),                      -- 制造业PMI:采购量
    pmi011200 DECIMAL(10,4),                      -- 制造业PMI:主要原材料购进价格
    pmi011300 DECIMAL(10,4),                      -- 制造业PMI:出厂价格
    pmi011400 DECIMAL(10,4),                      -- 制造业PMI:产成品库存
    pmi011500 DECIMAL(10,4),                      -- 制造业PMI:在手订单
    pmi011600 DECIMAL(10,4),                      -- 制造业PMI:生产经营活动预期
    pmi011700 DECIMAL(10,4),                      -- 制造业PMI:分行业/装备制造业
    pmi011800 DECIMAL(10,4),                      -- 制造业PMI:分行业/高技术制造业
    pmi011900 DECIMAL(10,4),                      -- 制造业PMI:分行业/基础原材料制造业
    pmi012000 DECIMAL(10,4),                      -- 制造业PMI:分行业/消费品制造业
    pmi020100 DECIMAL(10,4),                      -- 非制造业PMI:商务活动
    pmi020101 DECIMAL(10,4),                      -- 非制造业PMI:商务活动:建筑业
    pmi020102 DECIMAL(10,4),                      -- 非制造业PMI:商务活动:服务业
    pmi020200 DECIMAL(10,4),                      -- 非制造业PMI:新订单指数
    pmi020201 DECIMAL(10,4),                      -- 非制造业PMI:新订单指数:建筑业
    pmi020202 DECIMAL(10,4),                      -- 非制造业PMI:新订单指数:服务业
    pmi020300 DECIMAL(10,4),                      -- 非制造业PMI:投入品价格指数
    pmi020301 DECIMAL(10,4),                      -- 非制造业PMI:投入品价格指数:建筑业
    pmi020302 DECIMAL(10,4),                      -- 非制造业PMI:投入品价格指数:服务业
    pmi020400 DECIMAL(10,4),                      -- 非制造业PMI:销售价格指数
    pmi020401 DECIMAL(10,4),                      -- 非制造业PMI:销售价格指数:建筑业
    pmi020402 DECIMAL(10,4),                      -- 非制造业PMI:销售价格指数:服务业
    pmi020500 DECIMAL(10,4),                      -- 非制造业PMI:从业人员指数
    pmi020501 DECIMAL(10,4),                      -- 非制造业PMI:从业人员指数:建筑业
    pmi020502 DECIMAL(10,4),                      -- 非制造业PMI:从业人员指数:服务业
    pmi020600 DECIMAL(10,4),                      -- 非制造业PMI:业务活动预期指数
    pmi020601 DECIMAL(10,4),                      -- 非制造业PMI:业务活动预期指数:建筑业
    pmi020602 DECIMAL(10,4),                      -- 非制造业PMI:业务活动预期指数:服务业
    pmi020700 DECIMAL(10,4),                      -- 非制造业PMI:新出口订单
    pmi020800 DECIMAL(10,4),                      -- 非制造业PMI:在手订单
    pmi020900 DECIMAL(10,4),                      -- 非制造业PMI:存货
    pmi021000 DECIMAL(10,4),                      -- 非制造业PMI:供应商配送时间
    pmi030000 DECIMAL(10,4),                      -- 中国综合PMI:产出指数
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (time)
);

CREATE INDEX IF NOT EXISTS idx_cn_pmi_month ON cn_pmi(month);

COMMENT ON TABLE cn_pmi IS '中国PMI采购经理人指数数据表';
COMMENT ON COLUMN cn_pmi.time IS '月份末日期，格式如2024-01-31表示2024年1月末';
COMMENT ON COLUMN cn_pmi.month IS '月份，格式如202401表示2024年1月';
COMMENT ON COLUMN cn_pmi.pmi010000 IS '制造业PMI';

-- 指数日线行情数据表
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
    vol DECIMAL(30,6),                              -- 成交量（手）- 使用高精度避免大数溢出
    amount DECIMAL(30,6),                           -- 成交额（千元）- 使用高精度避免大数溢出
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

-- 大盘指数每日指标数据表
CREATE TABLE IF NOT EXISTS index_dailybasic (
    ts_code VARCHAR(20) NOT NULL,                   -- 指数代码，如 000001.SH（上证综指）
    trade_date TIMESTAMPTZ NOT NULL,                -- 交易日期
    total_mv DECIMAL(20,2),                         -- 当日总市值（元）
    float_mv DECIMAL(20,2),                         -- 当日流通市值（元）
    total_share DECIMAL(20,2),                      -- 当日总股本（股）
    float_share DECIMAL(20,2),                      -- 当日流通股本（股）
    free_share DECIMAL(20,2),                       -- 当日自由流通股本（股）
    turnover_rate DECIMAL(10,4),                    -- 换手率
    turnover_rate_f DECIMAL(10,4),                  -- 换手率(基于自由流通股本)
    pe DECIMAL(10,4),                               -- 市盈率
    pe_ttm DECIMAL(10,4),                           -- 市盈率TTM
    pb DECIMAL(10,4),                               -- 市净率
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_dailybasic_trade_date ON index_dailybasic(trade_date);

COMMENT ON TABLE index_dailybasic IS '大盘指数每日指标数据表 - 上证综指、深证成指、上证50、中证500、中小板指、创业板指';
COMMENT ON COLUMN index_dailybasic.ts_code IS '指数代码，如 000001.SH（上证综指）';
COMMENT ON COLUMN index_dailybasic.trade_date IS '交易日期';
COMMENT ON COLUMN index_dailybasic.total_mv IS '当日总市值（元）';
COMMENT ON COLUMN index_dailybasic.float_mv IS '当日流通市值（元）';
COMMENT ON COLUMN index_dailybasic.total_share IS '当日总股本（股）';
COMMENT ON COLUMN index_dailybasic.float_share IS '当日流通股本（股）';
COMMENT ON COLUMN index_dailybasic.free_share IS '当日自由流通股本（股）';
COMMENT ON COLUMN index_dailybasic.turnover_rate IS '换手率';
COMMENT ON COLUMN index_dailybasic.turnover_rate_f IS '换手率(基于自由流通股本)';
COMMENT ON COLUMN index_dailybasic.pe IS '市盈率';
COMMENT ON COLUMN index_dailybasic.pe_ttm IS '市盈率TTM';
COMMENT ON COLUMN index_dailybasic.pb IS '市净率';

-- 上市公司财务指标数据表
CREATE TABLE IF NOT EXISTS fina_indicator (
    ts_code VARCHAR(20) NOT NULL,                     -- TS代码
    ann_date VARCHAR(20) NOT NULL,                    -- 公告日期
    ann_date_time TIMESTAMPTZ,                        -- 公告日期（时间序列格式）
    end_date VARCHAR(20) NOT NULL,                    -- 报告期
    end_date_time TIMESTAMPTZ NOT NULL,               -- 报告期（时间序列格式，如2024-12-31）
    eps DECIMAL(20,4),                                -- 基本每股收益
    dt_eps DECIMAL(20,4),                             -- 稀释每股收益
    total_revenue_ps DECIMAL(20,4),                   -- 每股营业总收入
    revenue_ps DECIMAL(20,4),                         -- 每股营业收入
    capital_rese_ps DECIMAL(20,4),                    -- 每股资本公积
    surplus_rese_ps DECIMAL(20,4),                    -- 每股盈余公积
    undist_profit_ps DECIMAL(20,4),                   -- 每股未分配利润
    extra_item DECIMAL(20,4),                         -- 非经常性损益
    profit_dedt DECIMAL(20,4),                        -- 扣除非经常性损益后的净利润
    gross_margin DECIMAL(20,4),                       -- 毛利
    current_ratio DECIMAL(20,4),                      -- 流动比率
    quick_ratio DECIMAL(20,4),                        -- 速动比率
    cash_ratio DECIMAL(20,4),                         -- 保守速动比率
    invturn_days DECIMAL(20,4),                       -- 存货周转天数
    arturn_days DECIMAL(20,4),                        -- 应收账款周转天数
    inv_turn DECIMAL(20,4),                           -- 存货周转率
    ar_turn DECIMAL(20,4),                            -- 应收账款周转率
    ca_turn DECIMAL(20,4),                            -- 流动资产周转率
    fa_turn DECIMAL(20,4),                            -- 固定资产周转率
    assets_turn DECIMAL(20,4),                        -- 总资产周转率
    op_income DECIMAL(20,4),                          -- 经营活动净收益
    valuechange_income DECIMAL(20,4),                 -- 价值变动净收益
    interst_income DECIMAL(20,4),                     -- 利息费用
    daa DECIMAL(20,4),                                -- 折旧与摊销
    ebit DECIMAL(20,4),                               -- 息税前利润
    ebitda DECIMAL(20,4),                             -- 息税折旧摊销前利润
    fcff DECIMAL(20,4),                               -- 企业自由现金流量
    fcfe DECIMAL(20,4),                               -- 股权自由现金流量
    current_exint DECIMAL(20,4),                      -- 无息流动负债
    noncurrent_exint DECIMAL(20,4),                   -- 无息非流动负债
    interestdebt DECIMAL(20,4),                       -- 带息债务
    netdebt DECIMAL(20,4),                            -- 净债务
    tangible_asset DECIMAL(20,4),                     -- 有形资产
    working_capital DECIMAL(20,4),                    -- 营运资金
    networking_capital DECIMAL(20,4),                 -- 营运流动资本
    invest_capital DECIMAL(20,4),                     -- 全部投入资本
    retained_earnings DECIMAL(20,4),                  -- 留存收益
    diluted2_eps DECIMAL(20,4),                       -- 期末摊薄每股收益
    bps DECIMAL(20,4),                                -- 每股净资产
    ocfps DECIMAL(20,4),                              -- 每股经营活动产生的现金流量净额
    retainedps DECIMAL(20,4),                         -- 每股留存收益
    cfps DECIMAL(20,4),                               -- 每股现金流量净额
    ebit_ps DECIMAL(20,4),                            -- 每股息税前利润
    fcff_ps DECIMAL(20,4),                            -- 每股企业自由现金流量
    fcfe_ps DECIMAL(20,4),                            -- 每股股东自由现金流量
    netprofit_margin DECIMAL(20,4),                   -- 销售净利率
    grossprofit_margin DECIMAL(20,4),                 -- 销售毛利率
    cogs_of_sales DECIMAL(20,4),                      -- 销售成本率
    expense_of_sales DECIMAL(20,4),                   -- 销售期间费用率
    profit_to_gr DECIMAL(20,4),                       -- 净利润/营业总收入
    saleexp_to_gr DECIMAL(20,4),                      -- 销售费用/营业总收入
    adminexp_of_gr DECIMAL(20,4),                     -- 管理费用/营业总收入
    finaexp_of_gr DECIMAL(20,4),                      -- 财务费用/营业总收入
    impai_ttm DECIMAL(20,4),                          -- 资产减值损失/营业总收入
    gc_of_gr DECIMAL(20,4),                           -- 营业总成本/营业总收入
    op_of_gr DECIMAL(20,4),                           -- 营业利润/营业总收入
    ebit_of_gr DECIMAL(20,4),                         -- 息税前利润/营业总收入
    roe DECIMAL(20,4),                                -- 净资产收益率
    roe_waa DECIMAL(20,4),                            -- 加权平均净资产收益率
    roe_dt DECIMAL(20,4),                             -- 净资产收益率(扣除非经常损益)
    roa DECIMAL(20,4),                                -- 总资产报酬率
    npta DECIMAL(20,4),                               -- 总资产净利润
    roic DECIMAL(20,4),                               -- 投入资本回报率
    roe_yearly DECIMAL(20,4),                         -- 年化净资产收益率
    roa2_yearly DECIMAL(20,4),                        -- 年化总资产报酬率
    roe_avg DECIMAL(20,4),                            -- 平均净资产收益率(增发条件)
    opincome_of_ebt DECIMAL(20,4),                    -- 经营活动净收益/利润总额
    investincome_of_ebt DECIMAL(20,4),                -- 价值变动净收益/利润总额
    n_op_profit_of_ebt DECIMAL(20,4),                 -- 营业外收支净额/利润总额
    tax_to_ebt DECIMAL(20,4),                         -- 所得税/利润总额
    dtprofit_to_profit DECIMAL(20,4),                 -- 扣除非经常损益后的净利润/净利润
    salescash_to_or DECIMAL(20,4),                    -- 销售商品提供劳务收到的现金/营业收入
    ocf_to_or DECIMAL(20,4),                          -- 经营活动产生的现金流量净额/营业收入
    ocf_to_opincome DECIMAL(20,4),                    -- 经营活动产生的现金流量净额/经营活动净收益
    capitalized_to_da DECIMAL(20,4),                  -- 资本支出/折旧和摊销
    debt_to_assets DECIMAL(20,4),                     -- 资产负债率
    assets_to_eqt DECIMAL(20,4),                      -- 权益乘数
    dp_assets_to_eqt DECIMAL(20,4),                   -- 权益乘数(杜邦分析)
    ca_to_assets DECIMAL(20,4),                       -- 流动资产/总资产
    nca_to_assets DECIMAL(20,4),                      -- 非流动资产/总资产
    tbassets_to_totalassets DECIMAL(20,4),            -- 有形资产/总资产
    int_to_talcap DECIMAL(20,4),                      -- 带息债务/全部投入资本
    eqt_to_talcapital DECIMAL(20,4),                  -- 归属于母公司的股东权益/全部投入资本
    currentdebt_to_debt DECIMAL(20,4),                -- 流动负债/负债合计
    longdeb_to_debt DECIMAL(20,4),                    -- 非流动负债/负债合计
    ocf_to_shortdebt DECIMAL(20,4),                   -- 经营活动产生的现金流量净额/流动负债
    debt_to_eqt DECIMAL(20,4),                        -- 产权比率
    eqt_to_debt DECIMAL(20,4),                        -- 归属于母公司的股东权益/负债合计
    eqt_to_interestdebt DECIMAL(20,4),                -- 归属于母公司的股东权益/带息债务
    tangibleasset_to_debt DECIMAL(20,4),              -- 有形资产/负债合计
    tangasset_to_intdebt DECIMAL(20,4),               -- 有形资产/带息债务
    tangibleasset_to_netdebt DECIMAL(20,4),           -- 有形资产/净债务
    ocf_to_debt DECIMAL(20,4),                        -- 经营活动产生的现金流量净额/负债合计
    ocf_to_interestdebt DECIMAL(20,4),                -- 经营活动产生的现金流量净额/带息债务
    ocf_to_netdebt DECIMAL(20,4),                     -- 经营活动产生的现金流量净额/净债务
    ebit_to_interest DECIMAL(20,4),                   -- 已获利息倍数(EBIT/利息费用)
    longdebt_to_workingcapital DECIMAL(20,4),         -- 长期债务与营运资金比率
    ebitda_to_debt DECIMAL(20,4),                     -- 息税折旧摊销前利润/负债合计
    turn_days DECIMAL(20,4),                          -- 营业周期
    roa_yearly DECIMAL(20,4),                         -- 年化总资产净利率
    roa_dp DECIMAL(20,4),                             -- 总资产净利率(杜邦分析)
    fixed_assets DECIMAL(20,4),                       -- 固定资产合计
    profit_prefin_exp DECIMAL(20,4),                  -- 扣除财务费用前营业利润
    non_op_profit DECIMAL(20,4),                      -- 非营业利润
    op_to_ebt DECIMAL(20,4),                          -- 营业利润／利润总额
    nop_to_ebt DECIMAL(20,4),                         -- 非营业利润／利润总额
    ocf_to_profit DECIMAL(20,4),                      -- 经营活动产生的现金流量净额／营业利润
    cash_to_liqdebt DECIMAL(20,4),                    -- 货币资金／流动负债
    cash_to_liqdebt_withinterest DECIMAL(20,4),       -- 货币资金／带息流动负债
    op_to_liqdebt DECIMAL(20,4),                      -- 营业利润／流动负债
    op_to_debt DECIMAL(20,4),                         -- 营业利润／负债合计
    roic_yearly DECIMAL(20,4),                        -- 年化投入资本回报率
    total_fa_trun DECIMAL(20,4),                      -- 固定资产合计周转率
    profit_to_op DECIMAL(20,4),                       -- 利润总额／营业收入
    q_opincome DECIMAL(20,4),                         -- 经营活动单季度净收益
    q_investincome DECIMAL(20,4),                     -- 价值变动单季度净收益
    q_dtprofit DECIMAL(20,4),                         -- 扣除非经常损益后的单季度净利润
    q_eps DECIMAL(20,4),                              -- 每股收益(单季度)
    q_netprofit_margin DECIMAL(20,4),                 -- 销售净利率(单季度)
    q_gsprofit_margin DECIMAL(20,4),                  -- 销售毛利率(单季度)
    q_exp_to_sales DECIMAL(20,4),                     -- 销售期间费用率(单季度)
    q_profit_to_gr DECIMAL(20,4),                     -- 净利润／营业总收入(单季度)
    q_saleexp_to_gr DECIMAL(20,4),                    -- 销售费用／营业总收入(单季度)
    q_adminexp_to_gr DECIMAL(20,4),                   -- 管理费用／营业总收入(单季度)
    q_finaexp_to_gr DECIMAL(20,4),                    -- 财务费用／营业总收入(单季度)
    q_impair_to_gr_ttm DECIMAL(20,4),                 -- 资产减值损失／营业总收入(单季度)
    q_gc_to_gr DECIMAL(20,4),                         -- 营业总成本／营业总收入(单季度)
    q_op_to_gr DECIMAL(20,4),                         -- 营业利润／营业总收入(单季度)
    q_roe DECIMAL(20,4),                              -- 净资产收益率(单季度)
    q_dt_roe DECIMAL(20,4),                           -- 净资产单季度收益率(扣除非经常损益)
    q_npta DECIMAL(20,4),                             -- 总资产净利润(单季度)
    q_opincome_to_ebt DECIMAL(20,4),                  -- 经营活动净收益／利润总额(单季度)
    q_investincome_to_ebt DECIMAL(20,4),              -- 价值变动净收益／利润总额(单季度)
    q_dtprofit_to_profit DECIMAL(20,4),               -- 扣除非经常损益后的净利润／净利润(单季度)
    q_salescash_to_or DECIMAL(20,4),                  -- 销售商品提供劳务收到的现金／营业收入(单季度)
    q_ocf_to_sales DECIMAL(20,4),                     -- 经营活动产生的现金流量净额／营业收入(单季度)
    q_ocf_to_or DECIMAL(20,4),                        -- 经营活动产生的现金流量净额／经营活动净收益(单季度)
    basic_eps_yoy DECIMAL(20,4),                      -- 基本每股收益同比增长率(%)
    dt_eps_yoy DECIMAL(20,4),                         -- 稀释每股收益同比增长率(%)
    cfps_yoy DECIMAL(20,4),                           -- 每股经营活动产生的现金流量净额同比增长率(%)
    op_yoy DECIMAL(20,4),                             -- 营业利润同比增长率(%)
    ebt_yoy DECIMAL(20,4),                            -- 利润总额同比增长率(%)
    netprofit_yoy DECIMAL(20,4),                      -- 归属母公司股东的净利润同比增长率(%)
    dt_netprofit_yoy DECIMAL(20,4),                   -- 归属母公司股东的净利润-扣除非经常损益同比增长率(%)
    ocf_yoy DECIMAL(20,4),                            -- 经营活动产生的现金流量净额同比增长率(%)
    roe_yoy DECIMAL(20,4),                            -- 净资产收益率(摊薄)同比增长率(%)
    bps_yoy DECIMAL(20,4),                            -- 每股净资产相对年初增长率(%)
    assets_yoy DECIMAL(20,4),                         -- 资产总计相对年初增长率(%)
    eqt_yoy DECIMAL(20,4),                            -- 归属母公司的股东权益相对年初增长率(%)
    tr_yoy DECIMAL(20,4),                             -- 营业总收入同比增长率(%)
    or_yoy DECIMAL(20,4),                             -- 营业收入同比增长率(%)
    q_gr_yoy DECIMAL(20,4),                           -- 营业总收入同比增长率(%)(单季度)
    q_gr_qoq DECIMAL(20,4),                           -- 营业总收入环比增长率(%)(单季度)
    q_sales_yoy DECIMAL(20,4),                        -- 营业收入同比增长率(%)(单季度)
    q_sales_qoq DECIMAL(20,4),                        -- 营业收入环比增长率(%)(单季度)
    q_op_yoy DECIMAL(20,4),                           -- 营业利润同比增长率(%)(单季度)
    q_op_qoq DECIMAL(20,4),                           -- 营业利润环比增长率(%)(单季度)
    q_profit_yoy DECIMAL(20,4),                       -- 净利润同比增长率(%)(单季度)
    q_profit_qoq DECIMAL(20,4),                       -- 净利润环比增长率(%)(单季度)
    q_netprofit_yoy DECIMAL(20,4),                    -- 归属母公司股东的净利润同比增长率(%)(单季度)
    q_netprofit_qoq DECIMAL(20,4),                    -- 归属母公司股东的净利润环比增长率(%)(单季度)
    equity_yoy DECIMAL(20,4),                         -- 净资产同比增长率
    rd_exp DECIMAL(20,4),                             -- 研发费用
    update_flag VARCHAR(4),                           -- 更新标识
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date_time)
);

CREATE INDEX IF NOT EXISTS idx_fina_indicator_end_date ON fina_indicator(end_date_time);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date ON fina_indicator(ann_date);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date_time ON fina_indicator(ann_date_time);

COMMENT ON TABLE fina_indicator IS '上市公司财务指标数据表';
COMMENT ON COLUMN fina_indicator.ts_code IS 'TS代码';
COMMENT ON COLUMN fina_indicator.ann_date IS '公告日期';
COMMENT ON COLUMN fina_indicator.end_date IS '报告期';
COMMENT ON COLUMN fina_indicator.end_date_time IS '报告期（时间序列格式，用于增量更新）';
COMMENT ON COLUMN fina_indicator.eps IS '基本每股收益';
COMMENT ON COLUMN fina_indicator.roe IS '净资产收益率(%)';
COMMENT ON COLUMN fina_indicator.debt_to_assets IS '资产负债率(%)';

-- 上市公司现金流量表数据表
CREATE TABLE IF NOT EXISTS cashflow (
    ts_code VARCHAR(20) NOT NULL,                        -- TS代码
    ann_date VARCHAR(8),                                 -- 公告日期
    ann_date_time TIMESTAMPTZ,                           -- 公告日期（时间序列格式）
    f_ann_date VARCHAR(8),                               -- 实际公告日期
    f_ann_date_time TIMESTAMPTZ,                         -- 实际公告日期（时间序列格式）
    end_date VARCHAR(8),                                 -- 报告期
    end_date_time TIMESTAMPTZ NOT NULL,                  -- 报告期（时间序列格式，用于增量更新）
    comp_type VARCHAR(4),                                -- 公司类型
    report_type VARCHAR(4),                              -- 报表类型
    end_type VARCHAR(4),                                 -- 报告期类型
    -- 经营活动产生的现金流量
    net_profit DECIMAL(20,4),                            -- 净利润
    finan_exp DECIMAL(20,4),                             -- 财务费用
    c_fr_sale_sg DECIMAL(20,4),                          -- 销售商品、提供劳务收到的现金
    recp_tax_rends DECIMAL(20,4),                        -- 收到的税费返还
    n_depos_incr_fi DECIMAL(20,4),                       -- 客户存款和同业存放款项净增加额
    n_incr_loans_cb DECIMAL(20,4),                       -- 向中央银行借款净增加额
    n_inc_borr_oth_fi DECIMAL(20,4),                     -- 向其他金融机构拆入资金净增加额
    prem_fr_orig_contr DECIMAL(20,4),                    -- 收到原保险合同保费取得的现金
    n_incr_insured_dep DECIMAL(20,4),                    -- 保户储金净增加额
    n_reinsur_prem DECIMAL(20,4),                        -- 收到再保业务现金净额
    n_incr_disp_tfa DECIMAL(20,4),                       -- 处置交易性金融资产净增加额
    ifc_cash_incr DECIMAL(20,4),                         -- 收取利息和手续费净增加额
    n_incr_disp_faas DECIMAL(20,4),                      -- 处置可供出售金融资产净增加额
    n_incr_loans_oth_bank DECIMAL(20,4),                 -- 拆入资金净增加额
    n_cap_incr_repur DECIMAL(20,4),                      -- 回购业务资金净增加额
    c_fr_oth_operate_a DECIMAL(20,4),                    -- 收到其他与经营活动有关的现金
    c_inf_fr_operate_a DECIMAL(20,4),                    -- 经营活动现金流入小计
    c_paid_goods_s DECIMAL(20,4),                        -- 购买商品、接受劳务支付的现金
    c_paid_to_for_empl DECIMAL(20,4),                    -- 支付给职工以及为职工支付的现金
    c_paid_for_taxes DECIMAL(20,4),                      -- 支付的各项税费
    n_incr_clt_loan_adv DECIMAL(20,4),                   -- 客户贷款及垫款净增加额
    n_incr_dep_cbob DECIMAL(20,4),                       -- 存放央行和同业款项净增加额
    c_pay_claims_orig_inco DECIMAL(20,4),                -- 支付原保险合同赔付款项的现金
    pay_handling_chrg DECIMAL(20,4),                     -- 支付手续费的现金
    pay_comm_insur_plcy DECIMAL(20,4),                   -- 支付保单红利的现金
    oth_cash_pay_oper_act DECIMAL(20,4),                 -- 支付其他与经营活动有关的现金
    st_cash_out_act DECIMAL(20,4),                       -- 经营活动现金流出小计
    n_cashflow_act DECIMAL(20,4),                        -- 经营活动产生的现金流量净额
    -- 投资活动产生的现金流量
    oth_recp_ral_inv_act DECIMAL(20,4),                  -- 收到其他与投资活动有关的现金
    c_disp_withdrwl_invest DECIMAL(20,4),                -- 收回投资收到的现金
    c_recp_return_invest DECIMAL(20,4),                  -- 取得投资收益收到的现金
    n_recp_disp_fiolta DECIMAL(20,4),                    -- 处置固定资产、无形资产和其他长期资产收回的现金净额
    n_recp_disp_sobu DECIMAL(20,4),                      -- 处置子公司及其他营业单位收到的现金净额
    stot_inflows_inv_act DECIMAL(20,4),                  -- 投资活动现金流入小计
    c_pay_acq_const_fiolta DECIMAL(20,4),                -- 购建固定资产、无形资产和其他长期资产支付的现金
    c_paid_invest DECIMAL(20,4),                         -- 投资支付的现金
    n_disp_subs_oth_biz DECIMAL(20,4),                   -- 取得子公司及其他营业单位支付的现金净额
    oth_pay_ral_inv_act DECIMAL(20,4),                   -- 支付其他与投资活动有关的现金
    n_incr_pledge_loan DECIMAL(20,4),                    -- 质押贷款净增加额
    stot_out_inv_act DECIMAL(20,4),                      -- 投资活动现金流出小计
    n_cashflow_inv_act DECIMAL(20,4),                    -- 投资活动产生的现金流量净额
    -- 筹资活动产生的现金流量
    c_recp_borrow DECIMAL(20,4),                         -- 取得借款收到的现金
    proc_issue_bonds DECIMAL(20,4),                      -- 发行债券收到的现金
    oth_cash_recp_ral_fnc_act DECIMAL(20,4),             -- 收到其他与筹资活动有关的现金
    stot_cash_in_fnc_act DECIMAL(20,4),                  -- 筹资活动现金流入小计
    free_cashflow DECIMAL(20,4),                         -- 企业自由现金流量
    c_prepay_amt_borr DECIMAL(20,4),                     -- 偿还债务支付的现金
    c_pay_dist_dpcp_int_exp DECIMAL(20,4),               -- 分配股利、利润或偿付利息支付的现金
    incl_dvd_profit_paid_sc_ms DECIMAL(20,4),            -- 其中:子公司支付给少数股东的股利、利润
    oth_cashpay_ral_fnc_act DECIMAL(20,4),               -- 支付其他与筹资活动有关的现金
    stot_cashout_fnc_act DECIMAL(20,4),                  -- 筹资活动现金流出小计
    n_cash_flows_fnc_act DECIMAL(20,4),                  -- 筹资活动产生的现金流量净额
    -- 汇率变动对现金的影响
    eff_fx_flu_cash DECIMAL(20,4),                       -- 汇率变动对现金的影响
    n_incr_cash_cash_equ DECIMAL(20,4),                  -- 现金及现金等价物净增加额
    c_cash_equ_beg_period DECIMAL(20,4),                 -- 期初现金及现金等价物余额
    c_cash_equ_end_period DECIMAL(20,4),                 -- 期末现金及现金等价物余额
    -- 补充资料
    c_recp_cap_contrib DECIMAL(20,4),                    -- 吸收投资收到的现金
    incl_cash_rec_saims DECIMAL(20,4),                   -- 其中:子公司吸收少数股东投资收到的现金
    uncon_invest_loss DECIMAL(20,4),                     -- 未确认投资损失
    prov_depr_assets DECIMAL(20,4),                      -- 加:资产减值准备
    depr_fa_coga_dpba DECIMAL(20,4),                     -- 固定资产折旧、油气资产折耗、生产性生物资产折旧
    amort_intang_assets DECIMAL(20,4),                   -- 无形资产摊销
    lt_amort_deferred_exp DECIMAL(20,4),                 -- 长期待摊费用摊销
    decr_deferred_exp DECIMAL(20,4),                     -- 待摊费用减少
    incr_acc_exp DECIMAL(20,4),                          -- 预提费用增加
    loss_disp_fiolta DECIMAL(20,4),                      -- 处置固定、无形资产和其他长期资产的损失
    loss_scr_fa DECIMAL(20,4),                           -- 固定资产报废损失
    loss_fv_chg DECIMAL(20,4),                           -- 公允价值变动损失
    invest_loss DECIMAL(20,4),                           -- 投资损失
    decr_def_inc_tax_assets DECIMAL(20,4),               -- 递延所得税资产减少
    incr_def_inc_tax_liab DECIMAL(20,4),                 -- 递延所得税负债增加
    decr_inventories DECIMAL(20,4),                      -- 存货的减少
    decr_oper_payable DECIMAL(20,4),                     -- 经营性应收项目的减少
    incr_oper_payable DECIMAL(20,4),                     -- 经营性应付项目的增加
    others DECIMAL(20,4),                                -- 其他
    im_net_cashflow_oper_act DECIMAL(20,4),              -- 经营活动产生的现金流量净额(间接法)
    conv_debt_into_cap DECIMAL(20,4),                    -- 债务转为资本
    conv_copbonds_due_within_1y DECIMAL(20,4),           -- 一年内到期的可转换公司债券
    fa_fnc_leases DECIMAL(20,4),                         -- 融资租入固定资产
    im_n_incr_cash_equ DECIMAL(20,4),                    -- 现金及现金等价物净增加额(间接法)
    net_dism_capital_add DECIMAL(20,4),                  -- 拆出资金净增加额
    net_cash_rece_sec DECIMAL(20,4),                     -- 代理买卖证券收到的现金净额(元)
    credit_impa_loss DECIMAL(20,4),                      -- 信用减值损失
    use_right_asset_dep DECIMAL(20,4),                   -- 使用权资产折旧
    oth_loss_asset DECIMAL(20,4),                        -- 其他资产减值损失
    end_bal_cash DECIMAL(20,4),                          -- 现金的期末余额
    beg_bal_cash DECIMAL(20,4),                          -- 减:现金的期初余额
    end_bal_cash_equ DECIMAL(20,4),                      -- 加:现金等价物的期末余额
    beg_bal_cash_equ DECIMAL(20,4),                      -- 减:现金等价物的期初余额
    update_flag VARCHAR(4),                              -- 更新标志
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date_time)
);

CREATE INDEX IF NOT EXISTS idx_cashflow_end_date ON cashflow(end_date_time);
CREATE INDEX IF NOT EXISTS idx_cashflow_ann_date ON cashflow(ann_date);
CREATE INDEX IF NOT EXISTS idx_cashflow_ann_date_time ON cashflow(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_cashflow_f_ann_date_time ON cashflow(f_ann_date_time);
CREATE INDEX IF NOT EXISTS idx_cashflow_ts_code ON cashflow(ts_code);

COMMENT ON TABLE cashflow IS '上市公司现金流量表数据表';
COMMENT ON COLUMN cashflow.ts_code IS 'TS代码';
COMMENT ON COLUMN cashflow.ann_date IS '公告日期';
COMMENT ON COLUMN cashflow.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN cashflow.end_date IS '报告期';
COMMENT ON COLUMN cashflow.end_date_time IS '报告期（时间序列格式，用于增量更新）';
COMMENT ON COLUMN cashflow.comp_type IS '公司类型';
COMMENT ON COLUMN cashflow.net_profit IS '净利润';
COMMENT ON COLUMN cashflow.n_cashflow_act IS '经营活动产生的现金流量净额';
COMMENT ON COLUMN cashflow.n_cashflow_inv_act IS '投资活动产生的现金流量净额';
COMMENT ON COLUMN cashflow.n_cash_flows_fnc_act IS '筹资活动产生的现金流量净额';

-- 上市公司资产负债表数据表
CREATE TABLE IF NOT EXISTS balancesheet (
    ts_code VARCHAR(20) NOT NULL,                        -- TS代码
    ann_date VARCHAR(8),                                 -- 公告日期
    ann_date_time TIMESTAMPTZ,                           -- 公告日期（时间序列格式）
    f_ann_date VARCHAR(8),                               -- 实际公告日期
    f_ann_date_time TIMESTAMPTZ,                         -- 实际公告日期（时间序列格式）
    end_date VARCHAR(8),                                 -- 报告期
    end_date_time TIMESTAMPTZ NOT NULL,                  -- 报告期（时间序列格式，用于增量更新）
    comp_type VARCHAR(4),                                -- 公司类型
    report_type VARCHAR(4),                              -- 报表类型
    end_type VARCHAR(4),                                 -- 报告期类型
    -- 流动资产
    total_share DECIMAL(20,4),                           -- 期末总股本
    cap_rese DECIMAL(20,4),                              -- 资本公积金
    undistr_porfit DECIMAL(20,4),                        -- 未分配利润
    surplus_rese DECIMAL(20,4),                          -- 盈余公积金
    special_rese DECIMAL(20,4),                          -- 专项储备
    money_cap DECIMAL(20,4),                             -- 货币资金
    trad_asset DECIMAL(20,4),                            -- 交易性金融资产
    notes_receiv DECIMAL(20,4),                          -- 应收票据
    accounts_receiv DECIMAL(20,4),                       -- 应收账款
    oth_receiv DECIMAL(20,4),                            -- 其他应收款
    prepayment DECIMAL(20,4),                            -- 预付款项
    div_receiv DECIMAL(20,4),                            -- 应收股利
    int_receiv DECIMAL(20,4),                            -- 应收利息
    inventories DECIMAL(20,4),                           -- 存货
    amor_exp DECIMAL(20,4),                              -- 待摊费用
    nca_within_1y DECIMAL(20,4),                         -- 一年内到期的非流动资产
    sett_rsrv DECIMAL(20,4),                             -- 结算备付金
    loanto_oth_bank_fi DECIMAL(20,4),                    -- 拆出资金
    premium_receiv DECIMAL(20,4),                        -- 应收保费
    reinsur_receiv DECIMAL(20,4),                        -- 应收分保账款
    reinsur_res_receiv DECIMAL(20,4),                    -- 应收分保合同准备金
    pur_resale_fa DECIMAL(20,4),                         -- 买入返售金融资产
    oth_cur_assets DECIMAL(20,4),                        -- 其他流动资产
    total_cur_assets DECIMAL(20,4),                      -- 流动资产合计
    -- 非流动资产
    fa_avail_for_sale DECIMAL(20,4),                     -- 可供出售金融资产
    htm_invest DECIMAL(20,4),                            -- 持有至到期投资
    lt_eqt_invest DECIMAL(20,4),                         -- 长期股权投资
    invest_real_estate DECIMAL(20,4),                    -- 投资性房地产
    time_deposits DECIMAL(20,4),                         -- 定期存款
    oth_assets DECIMAL(20,4),                            -- 其他资产
    lt_rec DECIMAL(20,4),                                -- 长期应收款
    fix_assets DECIMAL(20,4),                            -- 固定资产
    cip DECIMAL(20,4),                                   -- 在建工程
    const_materials DECIMAL(20,4),                       -- 工程物资
    fixed_assets_disp DECIMAL(20,4),                     -- 固定资产清理
    produc_bio_assets DECIMAL(20,4),                     -- 生产性生物资产
    oil_and_gas_assets DECIMAL(20,4),                    -- 油气资产
    intan_assets DECIMAL(20,4),                          -- 无形资产
    r_and_d DECIMAL(20,4),                               -- 研发支出
    goodwill DECIMAL(20,4),                              -- 商誉
    lt_amor_exp DECIMAL(20,4),                           -- 长期待摊费用
    defer_tax_assets DECIMAL(20,4),                      -- 递延所得税资产
    decr_in_disbur DECIMAL(20,4),                        -- 发放贷款及垫款
    oth_nca DECIMAL(20,4),                               -- 其他非流动资产
    total_nca DECIMAL(20,4),                             -- 非流动资产合计
    -- 银行/保险特有资产
    cash_reser_cb DECIMAL(20,4),                         -- 现金及存放中央银行款项
    depos_in_oth_bfi DECIMAL(20,4),                      -- 存放同业和其它金融机构款项
    prec_metals DECIMAL(20,4),                           -- 贵金属
    deriv_assets DECIMAL(20,4),                          -- 衍生金融资产
    rr_reins_une_prem DECIMAL(20,4),                     -- 应收分保未到期责任准备金
    rr_reins_outstd_cla DECIMAL(20,4),                   -- 应收分保未决赔款准备金
    rr_reins_lins_liab DECIMAL(20,4),                    -- 应收分保寿险责任准备金
    rr_reins_lthins_liab DECIMAL(20,4),                  -- 应收分保长期健康险责任准备金
    refund_depos DECIMAL(20,4),                          -- 存出保证金
    ph_pledge_loans DECIMAL(20,4),                       -- 保户质押贷款
    refund_cap_depos DECIMAL(20,4),                      -- 存出资本保证金
    indept_acct_assets DECIMAL(20,4),                    -- 独立账户资产
    client_depos DECIMAL(20,4),                          -- 其中：客户资金存款
    client_prov DECIMAL(20,4),                           -- 其中：客户备付金
    transac_seat_fee DECIMAL(20,4),                      -- 其中:交易席位费
    invest_as_receiv DECIMAL(20,4),                      -- 应收款项类投资
    -- 资产总计
    total_assets DECIMAL(20,4),                          -- 资产总计
    -- 流动负债
    lt_borr DECIMAL(20,4),                               -- 长期借款
    st_borr DECIMAL(20,4),                               -- 短期借款
    cb_borr DECIMAL(20,4),                               -- 向中央银行借款
    depos_ib_deposits DECIMAL(20,4),                     -- 吸收存款及同业存放
    loan_oth_bank DECIMAL(20,4),                         -- 拆入资金
    trading_fl DECIMAL(20,4),                            -- 交易性金融负债
    notes_payable DECIMAL(20,4),                         -- 应付票据
    acct_payable DECIMAL(20,4),                          -- 应付账款
    adv_receipts DECIMAL(20,4),                          -- 预收款项
    sold_for_repur_fa DECIMAL(20,4),                     -- 卖出回购金融资产款
    comm_payable DECIMAL(20,4),                          -- 应付手续费及佣金
    payroll_payable DECIMAL(20,4),                       -- 应付职工薪酬
    taxes_payable DECIMAL(20,4),                         -- 应交税费
    int_payable DECIMAL(20,4),                           -- 应付利息
    div_payable DECIMAL(20,4),                           -- 应付股利
    oth_payable DECIMAL(20,4),                           -- 其他应付款
    acc_exp DECIMAL(20,4),                               -- 预提费用
    deferred_inc DECIMAL(20,4),                          -- 递延收益
    st_bonds_payable DECIMAL(20,4),                      -- 应付短期债券
    payable_to_reinsurer DECIMAL(20,4),                  -- 应付分保账款
    rsrv_insur_cont DECIMAL(20,4),                       -- 保险合同准备金
    acting_trading_sec DECIMAL(20,4),                    -- 代理买卖证券款
    acting_uw_sec DECIMAL(20,4),                         -- 代理承销证券款
    non_cur_liab_due_1y DECIMAL(20,4),                   -- 一年内到期的非流动负债
    oth_cur_liab DECIMAL(20,4),                          -- 其他流动负债
    total_cur_liab DECIMAL(20,4),                        -- 流动负债合计
    -- 非流动负债
    bond_payable DECIMAL(20,4),                          -- 应付债券
    lt_payable DECIMAL(20,4),                            -- 长期应付款
    specific_payables DECIMAL(20,4),                     -- 专项应付款
    estimated_liab DECIMAL(20,4),                        -- 预计负债
    defer_tax_liab DECIMAL(20,4),                        -- 递延所得税负债
    defer_inc_non_cur_liab DECIMAL(20,4),                -- 递延收益-非流动负债
    oth_ncl DECIMAL(20,4),                               -- 其他非流动负债
    total_ncl DECIMAL(20,4),                             -- 非流动负债合计
    -- 银行/保险特有负债
    depos_oth_bfi DECIMAL(20,4),                         -- 同业和其它金融机构存放款项
    deriv_liab DECIMAL(20,4),                            -- 衍生金融负债
    depos DECIMAL(20,4),                                 -- 吸收存款
    agency_bus_liab DECIMAL(20,4),                       -- 代理业务负债
    oth_liab DECIMAL(20,4),                              -- 其他负债
    prem_receiv_adva DECIMAL(20,4),                      -- 预收保费
    depos_received DECIMAL(20,4),                        -- 存入保证金
    ph_invest DECIMAL(20,4),                             -- 保户储金及投资款
    reser_une_prem DECIMAL(20,4),                        -- 未到期责任准备金
    reser_outstd_claims DECIMAL(20,4),                   -- 未决赔款准备金
    reser_lins_liab DECIMAL(20,4),                       -- 寿险责任准备金
    reser_lthins_liab DECIMAL(20,4),                     -- 长期健康险责任准备金
    indept_acc_liab DECIMAL(20,4),                       -- 独立账户负债
    pledge_borr DECIMAL(20,4),                           -- 其中:质押借款
    indem_payable DECIMAL(20,4),                         -- 应付赔付款
    policy_div_payable DECIMAL(20,4),                    -- 应付保单红利
    -- 负债合计
    total_liab DECIMAL(20,4),                            -- 负债合计
    -- 股东权益
    treasury_share DECIMAL(20,4),                        -- 减:库存股
    ordin_risk_reser DECIMAL(20,4),                      -- 一般风险准备
    forex_differ DECIMAL(20,4),                          -- 外币报表折算差额
    invest_loss_unconf DECIMAL(20,4),                    -- 未确认的投资损失
    minority_int DECIMAL(20,4),                          -- 少数股东权益
    total_hldr_eqy_exc_min_int DECIMAL(20,4),            -- 股东权益合计(不含少数股东权益)
    total_hldr_eqy_inc_min_int DECIMAL(20,4),            -- 股东权益合计(含少数股东权益)
    total_liab_hldr_eqy DECIMAL(20,4),                   -- 负债及股东权益总计
    -- 新增字段
    lt_payroll_payable DECIMAL(20,4),                    -- 长期应付职工薪酬
    oth_comp_income DECIMAL(20,4),                       -- 其他综合收益
    oth_eqt_tools DECIMAL(20,4),                         -- 其他权益工具
    oth_eqt_tools_p_shr DECIMAL(20,4),                   -- 其他权益工具(优先股)
    lending_funds DECIMAL(20,4),                         -- 融出资金
    acc_receivable DECIMAL(20,4),                        -- 应收款项
    st_fin_payable DECIMAL(20,4),                        -- 应付短期融资款
    payables DECIMAL(20,4),                              -- 应付款项
    hfs_assets DECIMAL(20,4),                            -- 持有待售的资产
    hfs_sales DECIMAL(20,4),                             -- 持有待售的负债
    cost_fin_assets DECIMAL(20,4),                       -- 以摊余成本计量的金融资产
    fair_value_fin_assets DECIMAL(20,4),                 -- 以公允价值计量且其变动计入其他综合收益的金融资产
    cip_total DECIMAL(20,4),                             -- 在建工程(合计)(元)
    oth_pay_total DECIMAL(20,4),                         -- 其他应付款(合计)(元)
    long_pay_total DECIMAL(20,4),                        -- 长期应付款(合计)(元)
    debt_invest DECIMAL(20,4),                           -- 债权投资(元)
    oth_debt_invest DECIMAL(20,4),                       -- 其他债权投资(元)
    oth_eq_invest DECIMAL(20,4),                         -- 其他权益工具投资(元)
    oth_illiq_fin_assets DECIMAL(20,4),                  -- 其他非流动金融资产(元)
    oth_eq_ppbond DECIMAL(20,4),                         -- 其他权益工具:永续债(元)
    receiv_financing DECIMAL(20,4),                      -- 应收款项融资
    use_right_assets DECIMAL(20,4),                      -- 使用权资产
    lease_liab DECIMAL(20,4),                            -- 租赁负债
    contract_assets DECIMAL(20,4),                       -- 合同资产
    contract_liab DECIMAL(20,4),                         -- 合同负债
    accounts_receiv_bill DECIMAL(20,4),                  -- 应收票据及应收账款
    accounts_pay DECIMAL(20,4),                          -- 应付票据及应付账款
    oth_rcv_total DECIMAL(20,4),                         -- 其他应收款(合计)（元）
    fix_assets_total DECIMAL(20,4),                      -- 固定资产(合计)(元)
    update_flag VARCHAR(4),                              -- 更新标志
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date_time)
);

CREATE INDEX IF NOT EXISTS idx_balancesheet_end_date ON balancesheet(end_date_time);
CREATE INDEX IF NOT EXISTS idx_balancesheet_ann_date ON balancesheet(ann_date);
CREATE INDEX IF NOT EXISTS idx_balancesheet_ann_date_time ON balancesheet(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_balancesheet_f_ann_date_time ON balancesheet(f_ann_date_time);
CREATE INDEX IF NOT EXISTS idx_balancesheet_ts_code ON balancesheet(ts_code);

COMMENT ON TABLE balancesheet IS '上市公司资产负债表数据表';
COMMENT ON COLUMN balancesheet.ts_code IS 'TS代码';
COMMENT ON COLUMN balancesheet.ann_date IS '公告日期';
COMMENT ON COLUMN balancesheet.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN balancesheet.end_date IS '报告期';
COMMENT ON COLUMN balancesheet.end_date_time IS '报告期（时间序列格式，用于增量更新）';
COMMENT ON COLUMN balancesheet.comp_type IS '公司类型';
COMMENT ON COLUMN balancesheet.total_assets IS '资产总计';
COMMENT ON COLUMN balancesheet.total_liab IS '负债合计';
COMMENT ON COLUMN balancesheet.total_hldr_eqy_exc_min_int IS '股东权益合计(不含少数股东权益)';

-- 上市公司利润表数据表
CREATE TABLE IF NOT EXISTS income (
    ts_code VARCHAR(20) NOT NULL,                           -- TS代码
    ann_date VARCHAR(8),                                    -- 公告日期
    ann_date_time TIMESTAMPTZ,                              -- 公告日期（时间序列格式）
    f_ann_date VARCHAR(8),                                  -- 实际公告日期
    f_ann_date_time TIMESTAMPTZ,                            -- 实际公告日期（时间序列格式）
    end_date VARCHAR(8),                                    -- 报告期
    end_date_time TIMESTAMPTZ NOT NULL,                     -- 报告期（时间序列格式，用于增量更新）
    comp_type VARCHAR(4),                                   -- 公司类型
    report_type VARCHAR(4),                                 -- 报表类型
    end_type VARCHAR(4),                                    -- 报告期类型
    -- 每股收益
    basic_eps DECIMAL(20,4),                                -- 基本每股收益
    diluted_eps DECIMAL(20,4),                              -- 稀释每股收益
    -- 收入类字段
    total_revenue DECIMAL(20,4),                            -- 营业总收入
    revenue DECIMAL(20,4),                                  -- 营业收入
    int_income DECIMAL(20,4),                               -- 利息收入
    prem_earned DECIMAL(20,4),                              -- 已赚保费
    comm_income DECIMAL(20,4),                              -- 手续费及佣金收入
    n_commis_income DECIMAL(20,4),                          -- 手续费及佣金净收入
    n_oth_income DECIMAL(20,4),                             -- 其他经营净收益
    n_oth_b_income DECIMAL(20,4),                           -- 加:其他业务净收益
    prem_income DECIMAL(20,4),                              -- 保险业务收入
    out_prem DECIMAL(20,4),                                 -- 减:分出保费
    une_prem_reser DECIMAL(20,4),                           -- 提取未到期责任准备金
    reins_income DECIMAL(20,4),                             -- 其中:分保费收入
    -- 证券业务
    n_sec_tb_income DECIMAL(20,4),                          -- 代理买卖证券业务净收入
    n_sec_uw_income DECIMAL(20,4),                          -- 证券承销业务净收入
    n_asset_mg_income DECIMAL(20,4),                        -- 受托客户资产管理业务净收入
    oth_b_income DECIMAL(20,4),                             -- 其他业务收入
    -- 投资收益
    fv_value_chg_gain DECIMAL(20,4),                        -- 加:公允价值变动净收益
    invest_income DECIMAL(20,4),                            -- 加:投资净收益
    ass_invest_income DECIMAL(20,4),                        -- 其中:对联营企业和合营企业的投资收益
    forex_gain DECIMAL(20,4),                               -- 加:汇兑净收益
    -- 成本费用
    total_cogs DECIMAL(20,4),                               -- 营业总成本
    oper_cost DECIMAL(20,4),                                -- 减:营业成本
    int_exp DECIMAL(20,4),                                  -- 减:利息支出
    comm_exp DECIMAL(20,4),                                 -- 减:手续费及佣金支出
    biz_tax_surchg DECIMAL(20,4),                           -- 减:营业税金及附加
    sell_exp DECIMAL(20,4),                                 -- 减:销售费用
    admin_exp DECIMAL(20,4),                                -- 减:管理费用
    fin_exp DECIMAL(20,4),                                  -- 减:财务费用
    assets_impair_loss DECIMAL(20,4),                       -- 减:资产减值损失
    -- 保险业务
    prem_refund DECIMAL(20,4),                              -- 退保金
    compens_payout DECIMAL(20,4),                           -- 赔付总支出
    reser_insur_liab DECIMAL(20,4),                         -- 提取保险责任准备金
    div_payt DECIMAL(20,4),                                 -- 保户红利支出
    reins_exp DECIMAL(20,4),                                -- 分保费用
    oper_exp DECIMAL(20,4),                                 -- 营业支出
    compens_payout_refu DECIMAL(20,4),                      -- 减:摊回赔付支出
    insur_reser_refu DECIMAL(20,4),                         -- 减:摊回保险责任准备金
    reins_cost_refund DECIMAL(20,4),                        -- 减:摊回分保费用
    other_bus_cost DECIMAL(20,4),                           -- 其他业务成本
    -- 利润
    operate_profit DECIMAL(20,4),                           -- 营业利润
    non_oper_income DECIMAL(20,4),                          -- 加:营业外收入
    non_oper_exp DECIMAL(20,4),                             -- 减:营业外支出
    nca_disploss DECIMAL(20,4),                             -- 其中:减:非流动资产处置净损失
    total_profit DECIMAL(20,4),                             -- 利润总额
    income_tax DECIMAL(20,4),                               -- 所得税费用
    n_income DECIMAL(20,4),                                 -- 净利润(含少数股东损益)
    n_income_attr_p DECIMAL(20,4),                          -- 净利润(不含少数股东损益)
    minority_gain DECIMAL(20,4),                            -- 少数股东损益
    -- 综合收益
    oth_compr_income DECIMAL(20,4),                         -- 其他综合收益
    t_compr_income DECIMAL(20,4),                           -- 综合收益总额
    compr_inc_attr_p DECIMAL(20,4),                         -- 归属于母公司(或股东)的综合收益总额
    compr_inc_attr_m_s DECIMAL(20,4),                       -- 归属于少数股东的综合收益总额
    -- 关键指标
    ebit DECIMAL(20,4),                                     -- 息税前利润
    ebitda DECIMAL(20,4),                                   -- 息税折旧摊销前利润
    -- 保险
    insurance_exp DECIMAL(20,4),                            -- 保险业务支出
    -- 利润分配
    undist_profit DECIMAL(20,4),                            -- 年初未分配利润
    distable_profit DECIMAL(20,4),                          -- 可分配利润
    -- 费用
    rd_exp DECIMAL(20,4),                                   -- 研发费用
    fin_exp_int_exp DECIMAL(20,4),                          -- 财务费用:利息费用
    fin_exp_int_inc DECIMAL(20,4),                          -- 财务费用:利息收入
    -- 盈余公积转入
    transfer_surplus_rese DECIMAL(20,4),                    -- 盈余公积转入
    transfer_housing_imprest DECIMAL(20,4),                 -- 住房周转金转入
    transfer_oth DECIMAL(20,4),                             -- 其他转入
    adj_lossgain DECIMAL(20,4),                             -- 调整以前年度损益
    -- 提取
    withdra_legal_surplus DECIMAL(20,4),                    -- 提取法定盈余公积
    withdra_legal_pubfund DECIMAL(20,4),                    -- 提取法定公益金
    withdra_biz_devfund DECIMAL(20,4),                      -- 提取企业发展基金
    withdra_rese_fund DECIMAL(20,4),                        -- 提取储备基金
    withdra_oth_ersu DECIMAL(20,4),                         -- 提取任意盈余公积金
    workers_welfare DECIMAL(20,4),                          -- 职工奖金福利
    distr_profit_shrhder DECIMAL(20,4),                     -- 可供股东分配的利润
    -- 应付股利
    prfshare_payable_dvd DECIMAL(20,4),                     -- 应付优先股股利
    comshare_payable_dvd DECIMAL(20,4),                     -- 应付普通股股利
    capit_comstock_div DECIMAL(20,4),                       -- 转作股本的普通股股利
    -- 新增字段
    net_after_nr_lp_correct DECIMAL(20,4),                  -- 扣除非经常性损益后的净利润（更正前）
    credit_impa_loss DECIMAL(20,4),                         -- 信用减值损失
    net_expo_hedging_benefits DECIMAL(20,4),                -- 净敞口套期收益
    oth_impair_loss_assets DECIMAL(20,4),                   -- 其他资产减值损失
    total_opcost DECIMAL(20,4),                             -- 营业总成本（二）
    amodcost_fin_assets DECIMAL(20,4),                      -- 以摊余成本计量的金融资产终止确认收益
    oth_income DECIMAL(20,4),                               -- 其他收益
    asset_disp_income DECIMAL(20,4),                        -- 资产处置收益
    continued_net_profit DECIMAL(20,4),                     -- 持续经营净利润
    end_net_profit DECIMAL(20,4),                           -- 终止经营净利润
    update_flag VARCHAR(4),                                 -- 更新标志
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date_time)
);

CREATE INDEX IF NOT EXISTS idx_income_end_date ON income(end_date_time);
CREATE INDEX IF NOT EXISTS idx_income_ann_date ON income(ann_date);
CREATE INDEX IF NOT EXISTS idx_income_ann_date_time ON income(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_income_f_ann_date_time ON income(f_ann_date_time);
CREATE INDEX IF NOT EXISTS idx_income_ts_code ON income(ts_code);

COMMENT ON TABLE income IS '上市公司利润表数据表';
COMMENT ON COLUMN income.ts_code IS 'TS代码';
COMMENT ON COLUMN income.ann_date IS '公告日期';
COMMENT ON COLUMN income.f_ann_date IS '实际公告日期';
COMMENT ON COLUMN income.end_date IS '报告期';
COMMENT ON COLUMN income.end_date_time IS '报告期（时间序列格式，用于增量更新）';
COMMENT ON COLUMN income.comp_type IS '公司类型';
COMMENT ON COLUMN income.report_type IS '报表类型';
COMMENT ON COLUMN income.basic_eps IS '基本每股收益';
COMMENT ON COLUMN income.diluted_eps IS '稀释每股收益';
COMMENT ON COLUMN income.total_revenue IS '营业总收入';
COMMENT ON COLUMN income.revenue IS '营业收入';
COMMENT ON COLUMN income.operate_profit IS '营业利润';
COMMENT ON COLUMN income.total_profit IS '利润总额';
COMMENT ON COLUMN income.income_tax IS '所得税费用';
COMMENT ON COLUMN income.n_income IS '净利润(含少数股东损益)';
COMMENT ON COLUMN income.ebit IS '息税前利润';
COMMENT ON COLUMN income.ebitda IS '息税折旧摊销前利润';


-- ======================================
-- 申万行业分类数据表
-- ======================================

-- 申万行业分类表
CREATE TABLE IF NOT EXISTS sw_industry_classify (
    index_code VARCHAR(30) NOT NULL,              -- 指数代码，如801010
    industry_name VARCHAR(100) NOT NULL,          -- 行业名称
    parent_code VARCHAR(30),                      -- 父级代码，L1的parent_code为0
    level VARCHAR(5) NOT NULL,                    -- 行业层级：L1/L2/L3
    industry_code VARCHAR(30) NOT NULL,           -- 行业代码
    is_pub VARCHAR(5),                            -- 是否发布指数
    src VARCHAR(20),                              -- 行业分类来源：SW2014/SW2021
    update_time TIMESTAMPTZ,                      -- 更新时间
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (index_code, industry_code)
);

CREATE INDEX IF NOT EXISTS idx_sw_classify_level ON sw_industry_classify(level);
CREATE INDEX IF NOT EXISTS idx_sw_classify_parent ON sw_industry_classify(parent_code);
CREATE INDEX IF NOT EXISTS idx_sw_classify_industry_code ON sw_industry_classify(industry_code);

COMMENT ON TABLE sw_industry_classify IS '申万行业分类表 - 存储申万2014/2021年版行业分类';
COMMENT ON COLUMN sw_industry_classify.index_code IS '指数代码，如801010';
COMMENT ON COLUMN sw_industry_classify.industry_name IS '行业名称';
COMMENT ON COLUMN sw_industry_classify.parent_code IS '父级代码，L1的parent_code为0';
COMMENT ON COLUMN sw_industry_classify.level IS '行业层级：L1/L2/L3';
COMMENT ON COLUMN sw_industry_classify.industry_code IS '行业代码，如801010表示一级行业，801010.SI表示二级';
COMMENT ON COLUMN sw_industry_classify.is_pub IS '是否发布指数';
COMMENT ON COLUMN sw_industry_classify.src IS '行业分类来源：SW2014（申万2014版）或SW2021（申万2021版）';

-- 申万行业成分股表
CREATE TABLE IF NOT EXISTS sw_industry_member (
    l1_code VARCHAR(30) NOT NULL,                 -- 一级行业代码，如801010
    l1_name VARCHAR(100) NOT NULL,                -- 一级行业名称
    l2_code VARCHAR(30) NOT NULL,                 -- 二级行业代码，如801010.SI
    l2_name VARCHAR(100) NOT NULL,                -- 二级行业名称
    l3_code VARCHAR(30) NOT NULL,                 -- 三级行业代码，如801010.SI10
    l3_name VARCHAR(100) NOT NULL,                -- 三级行业名称
    ts_code VARCHAR(20) NOT NULL,                 -- 成分股票代码，如600519.SH
    name VARCHAR(100),                            -- 成分股票名称
    in_date DATE,                                 -- 纳入日期
    out_date DATE,                                -- 剔除日期
    is_new VARCHAR(5),                            -- 是否最新：Y/N
    update_time TIMESTAMPTZ,                      -- 更新时间
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (l3_code, ts_code)
);

CREATE INDEX IF NOT EXISTS idx_sw_member_l1 ON sw_industry_member(l1_code);
CREATE INDEX IF NOT EXISTS idx_sw_member_l2 ON sw_industry_member(l2_code);
CREATE INDEX IF NOT EXISTS idx_sw_member_l3 ON sw_industry_member(l3_code);
CREATE INDEX IF NOT EXISTS idx_sw_member_ts_code ON sw_industry_member(ts_code);
CREATE INDEX IF NOT EXISTS idx_sw_member_in_date ON sw_industry_member(in_date);
CREATE INDEX IF NOT EXISTS idx_sw_member_out_date ON sw_industry_member(out_date);

COMMENT ON TABLE sw_industry_member IS '申万行业成分股表 - 存储申万行业分类的成分股票';
COMMENT ON COLUMN sw_industry_member.l1_code IS '一级行业代码，如801010';
COMMENT ON COLUMN sw_industry_member.l1_name IS '一级行业名称';
COMMENT ON COLUMN sw_industry_member.l2_code IS '二级行业代码，如801010.SI';
COMMENT ON COLUMN sw_industry_member.l2_name IS '二级行业名称';
COMMENT ON COLUMN sw_industry_member.l3_code IS '三级行业代码，如801010.SI10';
COMMENT ON COLUMN sw_industry_member.l3_name IS '三级行业名称';
COMMENT ON COLUMN sw_industry_member.ts_code IS '成分股票代码，如600519.SH';
COMMENT ON COLUMN sw_industry_member.name IS '成分股票名称';
COMMENT ON COLUMN sw_industry_member.in_date IS '纳入日期';
COMMENT ON COLUMN sw_industry_member.out_date IS '剔除日期';
COMMENT ON COLUMN sw_industry_member.is_new IS '是否最新：Y-是，N-否';

-- 申万行业日线行情数据表
CREATE TABLE IF NOT EXISTS sw_daily (
    ts_code VARCHAR(20) NOT NULL,                     -- 行业代码，如 801780.SI
    trade_date TIMESTAMPTZ NOT NULL,                  -- 交易日期（时间序列类型）
    name VARCHAR(100),                                -- 指数名称
    open DECIMAL(20, 4),                              -- 开盘点位
    low DECIMAL(20, 4),                               -- 最低点位
    high DECIMAL(20, 4),                              -- 最高点位
    close DECIMAL(20, 4),                             -- 收盘点位
    change DECIMAL(20, 4),                            -- 涨跌点位
    pct_change DECIMAL(20, 6),                        -- 涨跌幅
    vol DECIMAL(20, 4),                               -- 成交量（万股）
    amount DECIMAL(20, 4),                            -- 成交额（万元）
    pe DECIMAL(20, 4),                                -- 市盈率
    pb DECIMAL(20, 4),                                -- 市净率
    float_mv DECIMAL(20, 4),                          -- 流通市值（万元）
    total_mv DECIMAL(20, 4),                          -- 总市值（万元）
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);

COMMENT ON TABLE sw_daily IS '申万行业日线行情数据表 - 存储申万行业指数的日线行情数据（TimescaleDB超表）';
COMMENT ON COLUMN sw_daily.ts_code IS '行业代码，如801780.SI（申万农林牧渔指数）';
COMMENT ON COLUMN sw_daily.trade_date IS '交易日期';
COMMENT ON COLUMN sw_daily.name IS '指数名称';
COMMENT ON COLUMN sw_daily.open IS '开盘点位';
COMMENT ON COLUMN sw_daily.close IS '收盘点位';
COMMENT ON COLUMN sw_daily.pct_change IS '涨跌幅（%）';
COMMENT ON COLUMN sw_daily.vol IS '成交量（万股）';
COMMENT ON COLUMN sw_daily.amount IS '成交额（万元）';
COMMENT ON COLUMN sw_daily.pe IS '市盈率';
COMMENT ON COLUMN sw_daily.pb IS '市净率';
COMMENT ON COLUMN sw_daily.float_mv IS '流通市值（万元）';
COMMENT ON COLUMN sw_daily.total_mv IS '总市值（万元）';

-- ======================================
-- 交易日历数据表
-- ======================================

-- 交易日历表
CREATE TABLE IF NOT EXISTS trade_cal (
    exchange VARCHAR(10) NOT NULL,              -- 交易所代码：SSE/SZSE/CFFEX/SHFE/CZCE/DCE/INE
    cal_date TIMESTAMPTZ NOT NULL,              -- 日历日期
    is_open INTEGER NOT NULL,                   -- 是否交易：0-休市，1-交易
    pretrade_date TIMESTAMPTZ,                  -- 上一个交易日
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (exchange, cal_date)
);

CREATE INDEX IF NOT EXISTS idx_trade_cal_cal_date ON trade_cal(cal_date);
CREATE INDEX IF NOT EXISTS idx_trade_cal_exchange ON trade_cal(exchange);
CREATE INDEX IF NOT EXISTS idx_trade_cal_is_open ON trade_cal(is_open);

COMMENT ON TABLE trade_cal IS '交易日历数据表 - 各大交易所交易日历';
COMMENT ON COLUMN trade_cal.exchange IS '交易所代码：SSE(上交所)、SZSE(深交所)、CFFEX(中金所)、SHFE(上期所)、CZCE(郑商所)、DCE(大商所)、INE(上能源)';
COMMENT ON COLUMN trade_cal.cal_date IS '日历日期';
COMMENT ON COLUMN trade_cal.is_open IS '是否交易：0-休市，1-交易';
COMMENT ON COLUMN trade_cal.pretrade_date IS '上一个交易日';

-- ======================================
-- 指数成分权重数据表
-- ======================================

-- 指数成分权重表（月度数据）
CREATE TABLE IF NOT EXISTS index_weight (
    index_code VARCHAR(20) NOT NULL,              -- 指数代码，如 000300.SH（沪深300）
    con_code VARCHAR(20) NOT NULL,                -- 成分代码
    trade_date DATE NOT NULL,                     -- 交易日期（月度数据）
    weight DECIMAL(10, 6),                        -- 权重
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (index_code, con_code, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_index_weight_trade_date ON index_weight(trade_date);
CREATE INDEX IF NOT EXISTS idx_index_weight_index_code ON index_weight(index_code);

COMMENT ON TABLE index_weight IS '指数成分权重数据表 - 各类指数成分和权重（月度数据）';
COMMENT ON COLUMN index_weight.index_code IS '指数代码，如000300.SH(沪深300)、000905.SH(中证500)';
COMMENT ON COLUMN index_weight.con_code IS '成分代码';
COMMENT ON COLUMN index_weight.trade_date IS '交易日期（月度数据）';
COMMENT ON COLUMN index_weight.weight IS '权重';
