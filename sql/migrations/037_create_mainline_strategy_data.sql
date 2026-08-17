-- 量化主线策略数据层（原始补充 + 因子宽表）。
-- 注意：本迁移只创建数据结构，不计算策略分数，也不生成投资组合。
-- 执行方式（由数据库管理员手动执行）：
--   psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f sql/migrations/037_create_mainline_strategy_data.sql

BEGIN;

-- 历史申万成分必须允许同一股票多次进入同一行业。
ALTER TABLE sw_industry_member
    ADD COLUMN IF NOT EXISTS membership_id BIGSERIAL;
ALTER TABLE sw_industry_member
    ADD COLUMN IF NOT EXISTS in_date_key DATE
    GENERATED ALWAYS AS (COALESCE(in_date, DATE '1900-01-01')) STORED;
ALTER TABLE sw_industry_member DROP CONSTRAINT IF EXISTS sw_industry_member_pkey;
ALTER TABLE sw_industry_member
    ADD CONSTRAINT sw_industry_member_pkey PRIMARY KEY (membership_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_sw_member_history
    ON sw_industry_member(l3_code, ts_code, in_date_key);
CREATE INDEX IF NOT EXISTS idx_sw_member_point_in_time
    ON sw_industry_member(ts_code, in_date, out_date);

CREATE TABLE IF NOT EXISTS stock_st (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    name VARCHAR(100),
    type VARCHAR(32),
    type_name VARCHAR(100),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_stock_st_trade_date ON stock_st(trade_date);

CREATE TABLE IF NOT EXISTS stock_namechange (
    ts_code VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    start_date DATE NOT NULL,
    end_date DATE,
    ann_date DATE,
    change_reason TEXT,
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, start_date, name)
);
CREATE INDEX IF NOT EXISTS idx_stock_namechange_asof
    ON stock_namechange(ts_code, start_date, end_date);

CREATE TABLE IF NOT EXISTS stock_suspend (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    suspend_timing TEXT,
    suspend_type VARCHAR(64) NOT NULL DEFAULT '',
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date, suspend_type)
);
CREATE INDEX IF NOT EXISTS idx_stock_suspend_trade_date ON stock_suspend(trade_date);

CREATE TABLE IF NOT EXISTS stock_dividend (
    ts_code VARCHAR(20) NOT NULL,
    end_date DATE NOT NULL,
    ann_date DATE NOT NULL,
    div_proc VARCHAR(64) NOT NULL DEFAULT '',
    stk_div NUMERIC(24,10),
    stk_bo_rate NUMERIC(24,10),
    stk_co_rate NUMERIC(24,10),
    cash_div NUMERIC(24,10),
    cash_div_tax NUMERIC(24,10),
    record_date DATE,
    ex_date DATE,
    pay_date DATE,
    div_listdate DATE,
    imp_ann_date DATE,
    base_date DATE,
    base_share NUMERIC(30,6),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, end_date, ann_date, div_proc)
);
CREATE INDEX IF NOT EXISTS idx_stock_dividend_ex_date ON stock_dividend(ex_date);

CREATE TABLE IF NOT EXISTS stock_repurchase (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE,
    proc VARCHAR(64) NOT NULL DEFAULT '',
    exp_date DATE,
    vol NUMERIC(30,6),
    amount NUMERIC(30,6),
    high_limit NUMERIC(24,8),
    low_limit NUMERIC(24,8),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, ann_date, proc)
);
CREATE INDEX IF NOT EXISTS idx_stock_repurchase_end_date ON stock_repurchase(end_date);

CREATE TABLE IF NOT EXISTS margin_detail (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    name VARCHAR(100),
    rzye NUMERIC(30,6), rqye NUMERIC(30,6), rzmre NUMERIC(30,6),
    rqyl NUMERIC(30,6), rzche NUMERIC(30,6), rqchl NUMERIC(30,6),
    rqmcl NUMERIC(30,6), rzrqye NUMERIC(30,6),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_margin_detail_trade_date ON margin_detail(trade_date);

CREATE TABLE IF NOT EXISTS moneyflow_hsgt (
    trade_date DATE PRIMARY KEY,
    ggt_ss NUMERIC(24,6), ggt_sz NUMERIC(24,6),
    hgt NUMERIC(24,6), sgt NUMERIC(24,6),
    north_money NUMERIC(24,6), south_money NUMERIC(24,6),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_mainline_stock_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    l1_code VARCHAR(30), l1_name VARCHAR(100),
    l2_code VARCHAR(30), l2_name VARCHAR(100),
    is_listed BOOLEAN, is_st BOOLEAN, st_source VARCHAR(32),
    is_suspended BOOLEAN, is_eligible BOOLEAN,
    exclusion_reason TEXT,
    listing_days INTEGER,
    close NUMERIC(24,8), amount NUMERIC(30,6),
    total_mv NUMERIC(30,6), circ_mv NUMERIC(30,6),
    turnover_rate NUMERIC(24,8), pe_ttm NUMERIC(24,8),
    pb NUMERIC(24,8), dv_ttm NUMERIC(24,8),
    roe_ttm NUMERIC(24,8), roa_ttm NUMERIC(24,8), roic NUMERIC(24,8),
    grossprofit_margin NUMERIC(24,8),
    revenue_yoy NUMERIC(24,8), profit_yoy NUMERIC(24,8),
    ocf_to_profit NUMERIC(24,8), debt_to_assets NUMERIC(24,8),
    return_20d NUMERIC(24,10), return_60d NUMERIC(24,10),
    return_120d NUMERIC(24,10), volatility_20d NUMERIC(24,10),
    drawdown_120d NUMERIC(24,10),
    amount_pct_20d NUMERIC(24,10), turnover_pct_20d NUMERIC(24,10),
    pe_pct_5y NUMERIC(24,10), pb_pct_5y NUMERIC(24,10),
    rzye NUMERIC(30,6), rqye NUMERIC(30,6), rzmre NUMERIC(30,6),
    dividend_event_120d BOOLEAN, repurchase_event_120d BOOLEAN,
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_date_eligible
    ON processed_mainline_stock_daily(trade_date, is_eligible);
CREATE INDEX IF NOT EXISTS idx_mainline_stock_l2_date
    ON processed_mainline_stock_daily(l2_code, trade_date);

CREATE TABLE IF NOT EXISTS processed_mainline_market_daily (
    trade_date DATE PRIMARY KEY,
    benchmark_code VARCHAR(32) NOT NULL DEFAULT '000985.CSI',
    benchmark_close NUMERIC(24,8), benchmark_return_20d NUMERIC(24,10),
    benchmark_return_60d NUMERIC(24,10), benchmark_return_120d NUMERIC(24,10),
    benchmark_ma20_gap NUMERIC(24,10), benchmark_ma60_gap NUMERIC(24,10),
    benchmark_volatility_20d NUMERIC(24,10),
    breadth_above_ma20 NUMERIC(24,10), breadth_above_ma60 NUMERIC(24,10),
    advance_decline_ratio NUMERIC(24,10),
    north_money NUMERIC(24,6), north_money_20d NUMERIC(30,6),
    market_regime VARCHAR(32),
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_mainline_industry_daily (
    trade_date DATE NOT NULL,
    l1_code VARCHAR(30), l1_name VARCHAR(100),
    l2_code VARCHAR(30) NOT NULL, l2_name VARCHAR(100),
    stock_count INTEGER,
    equal_weight_return NUMERIC(24,10), cap_weight_return NUMERIC(24,10),
    relative_return_20d NUMERIC(24,10), relative_return_60d NUMERIC(24,10),
    breadth_above_ma20 NUMERIC(24,10), breadth_above_ma60 NUMERIC(24,10),
    return_dispersion NUMERIC(24,10), amount_share NUMERIC(24,10),
    median_pe_ttm NUMERIC(24,8), median_pb NUMERIC(24,8),
    median_roe_ttm NUMERIC(24,8), median_roic NUMERIC(24,8),
    median_grossprofit_margin NUMERIC(24,8),
    median_revenue_yoy NUMERIC(24,8), median_profit_yoy NUMERIC(24,8),
    margin_balance_change_20d NUMERIC(30,6),
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (l2_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_mainline_industry_l1_date
    ON processed_mainline_industry_daily(l1_code, trade_date);

CREATE TABLE IF NOT EXISTS processed_mainline_etf_daily (
    trade_date DATE NOT NULL,
    ts_code VARCHAR(32) NOT NULL,
    index_code VARCHAR(32),
    benchmark_available BOOLEAN NOT NULL DEFAULT FALSE,
    is_eligible BOOLEAN NOT NULL DEFAULT FALSE,
    exclusion_reason TEXT,
    adj_close NUMERIC(24,8), return_20d NUMERIC(24,10),
    return_60d NUMERIC(24,10), return_120d NUMERIC(24,10),
    volatility_20d NUMERIC(24,10), amount NUMERIC(30,6),
    amount_pct_20d NUMERIC(24,10), total_share NUMERIC(30,6),
    total_size NUMERIC(30,6), share_change_5d NUMERIC(30,6),
    share_change_20d NUMERIC(30,6), tracking_error_60d NUMERIC(24,10),
    premium_proxy NUMERIC(24,10),
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_mainline_etf_date_eligible
    ON processed_mainline_etf_daily(trade_date, is_eligible);

CREATE TABLE IF NOT EXISTS processed_mainline_fund_crowding_monthly (
    report_period DATE NOT NULL,
    available_date DATE NOT NULL,
    ts_code VARCHAR(20) NOT NULL,
    fund_count INTEGER,
    holding_value NUMERIC(30,6), holding_ratio NUMERIC(24,10),
    crowding_pct NUMERIC(24,10),
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (report_period, ts_code)
);
CREATE INDEX IF NOT EXISTS idx_mainline_crowding_available
    ON processed_mainline_fund_crowding_monthly(available_date, ts_code);

CREATE TABLE IF NOT EXISTS processed_mainline_leadlag_monthly (
    month_end DATE NOT NULL,
    leader_type VARCHAR(16) NOT NULL,
    leader_code VARCHAR(32) NOT NULL,
    follower_type VARCHAR(16) NOT NULL,
    follower_code VARCHAR(32) NOT NULL,
    best_lag_days INTEGER NOT NULL,
    correlation NUMERIC(24,10), sample_count INTEGER,
    source_asof TIMESTAMPTZ NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (month_end, leader_type, leader_code, follower_type, follower_code)
);

CREATE TABLE IF NOT EXISTS processed_mainline_data_status (
    dataset VARCHAR(64) NOT NULL,
    partition_date DATE NOT NULL,
    row_count BIGINT NOT NULL DEFAULT 0,
    eligible_count BIGINT,
    excluded_count BIGINT,
    max_source_date DATE,
    completeness NUMERIC(12,8),
    status VARCHAR(16) NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (dataset, partition_date)
);

COMMENT ON TABLE processed_mainline_stock_daily IS '主线策略股票日频因子；仅提供事实与因子，不包含策略评分';
COMMENT ON TABLE processed_mainline_etf_daily IS '主线策略 ETF 日频因子；缺基准的 ETF 明确排除且记录原因';
COMMENT ON TABLE processed_mainline_fund_crowding_monthly IS '按披露可用日控制的公募基金持仓拥挤度';
COMMENT ON TABLE processed_mainline_data_status IS '主线策略数据覆盖率、新鲜度和排除原因审计';

COMMIT;
