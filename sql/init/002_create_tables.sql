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
    end_date VARCHAR(20) NOT NULL,                    -- 报告期
    end_date_time TIMESTAMPTZ NOT NULL,               -- 报告期（时间序列格式，如2024-12-31）
    eps DECIMAL(15,4),                                -- 基本每股收益
    dt_eps DECIMAL(15,4),                             -- 稀释每股收益
    total_revenue_ps DECIMAL(15,4),                   -- 每股营业总收入
    revenue_ps DECIMAL(15,4),                         -- 每股营业收入
    capital_rese_ps DECIMAL(15,4),                    -- 每股资本公积
    surplus_rese_ps DECIMAL(15,4),                    -- 每股盈余公积
    undist_profit_ps DECIMAL(15,4),                   -- 每股未分配利润
    extra_item DECIMAL(20,4),                         -- 非经常性损益
    profit_dedt DECIMAL(20,4),                        -- 扣除非经常性损益后的净利润
    gross_margin DECIMAL(20,4),                       -- 毛利
    current_ratio DECIMAL(20,4),                      -- 流动比率
    quick_ratio DECIMAL(20,4),                        -- 速动比率
    cash_ratio DECIMAL(20,4),                         -- 保守速动比率
    ar_turn DECIMAL(20,4),                            -- 应收账款周转率
    ca_turn DECIMAL(20,4),                            -- 流动资产周转率
    fa_turn DECIMAL(20,4),                            -- 固定资产周转率
    assets_turn DECIMAL(20,4),                        -- 总资产周转率
    op_income DECIMAL(20,4),                          -- 经营活动净收益
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
    diluted2_eps DECIMAL(15,4),                       -- 期末摊薄每股收益
    bps DECIMAL(15,4),                                -- 每股净资产
    ocfps DECIMAL(15,4),                              -- 每股经营活动产生的现金流量净额
    cfps DECIMAL(15,4),                               -- 每股现金流量净额
    ebit_ps DECIMAL(15,4),                            -- 每股息税前利润
    netprofit_margin DECIMAL(10,4),                   -- 销售净利率
    grossprofit_margin DECIMAL(10,4),                 -- 销售毛利率
    profit_to_gr DECIMAL(10,4),                       -- 净利润/营业总收入
    roe DECIMAL(10,4),                                -- 净资产收益率
    roe_waa DECIMAL(10,4),                            -- 加权平均净资产收益率
    roe_dt DECIMAL(10,4),                             -- 净资产收益率(扣除非经常损益)
    roa DECIMAL(10,4),                                -- 总资产报酬率
    roic DECIMAL(10,4),                               -- 投入资本回报率
    debt_to_assets DECIMAL(10,4),                     -- 资产负债率
    assets_to_eqt DECIMAL(10,4),                      -- 权益乘数
    ca_to_assets DECIMAL(10,4),                       -- 流动资产/总资产
    nca_to_assets DECIMAL(10,4),                      -- 非流动资产/总资产
    tbassets_to_totalassets DECIMAL(10,4),            -- 有形资产/总资产
    int_to_talcap DECIMAL(10,4),                      -- 带息债务/全部投入资本
    eqt_to_talcapital DECIMAL(10,4),                  -- 归属于母公司的股东权益/全部投入资本
    currentdebt_to_debt DECIMAL(10,4),                -- 流动负债/负债合计
    longdeb_to_debt DECIMAL(10,4),                    -- 非流动负债/负债合计
    debt_to_eqt DECIMAL(10,4),                        -- 产权比率
    eqt_to_debt DECIMAL(10,4),                        -- 归属于母公司的股东权益/负债合计
    eqt_to_interestdebt DECIMAL(10,4),                -- 归属于母公司的股东权益/带息债务
    tangibleasset_to_debt DECIMAL(10,4),              -- 有形资产/负债合计
    ocf_to_debt DECIMAL(10,4),                        -- 经营活动产生的现金流量净额/负债合计
    turn_days DECIMAL(10,2),                          -- 营业周期
    fixed_assets DECIMAL(20,4),                       -- 固定资产合计
    profit_prefin_exp DECIMAL(20,4),                  -- 扣除财务费用前营业利润
    non_op_profit DECIMAL(20,4),                      -- 非营业利润
    op_to_ebt DECIMAL(10,4),                          -- 营业利润／利润总额
    q_opincome DECIMAL(20,4),                         -- 经营活动单季度净收益
    q_dtprofit DECIMAL(20,4),                         -- 扣除非经常损益后的单季度净利润
    q_eps DECIMAL(15,4),                              -- 每股收益(单季度)
    q_netprofit_margin DECIMAL(10,4),                 -- 销售净利率(单季度)
    q_gsprofit_margin DECIMAL(10,4),                  -- 销售毛利率(单季度)
    q_profit_to_gr DECIMAL(10,4),                     -- 净利润／营业总收入(单季度)
    q_salescash_to_or DECIMAL(10,4),                  -- 销售商品提供劳务收到的现金／营业收入(单季度)
    q_ocf_to_sales DECIMAL(10,4),                     -- 经营活动产生的现金流量净额／营业收入(单季度)
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
    q_sales_yoy DECIMAL(20,4),                        -- 营业收入同比增长率(%)(单季度)
    q_op_yoy DECIMAL(20,4),                           -- 营业利润同比增长率(%)(单季度)
    q_op_qoq DECIMAL(20,4),                           -- 营业利润环比增长率(%)(单季度)
    q_profit_yoy DECIMAL(20,4),                       -- 净利润同比增长率(%)(单季度)
    q_profit_qoq DECIMAL(20,4),                       -- 净利润环比增长率(%)(单季度)
    q_netprofit_yoy DECIMAL(20,4),                    -- 归属母公司股东的净利润同比增长率(%)(单季度)
    q_netprofit_qoq DECIMAL(20,4),                    -- 归属母公司股东的净利润环比增长率(%)(单季度)
    equity_yoy DECIMAL(20,4),                         -- 净资产同比增长率
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date_time)
);

CREATE INDEX IF NOT EXISTS idx_fina_indicator_end_date ON fina_indicator(end_date_time);
CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date ON fina_indicator(ann_date);

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
    f_ann_date VARCHAR(8),                               -- 实际公告日期
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
