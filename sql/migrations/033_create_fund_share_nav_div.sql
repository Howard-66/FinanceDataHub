-- Migration: 033_create_fund_share_nav_div
-- Description: Persist all output fields of Tushare fund_share, fund_nav and fund_div.

CREATE TABLE IF NOT EXISTS fund_share (
    ts_code VARCHAR(20) NOT NULL, trade_date DATE NOT NULL,
    fd_share DECIMAL(24, 6), updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_fund_share_trade_date ON fund_share(trade_date);

CREATE TABLE IF NOT EXISTS fund_nav (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE, nav_date DATE NOT NULL,
    unit_nav DECIMAL(24, 8), accum_nav DECIMAL(24, 8), accum_div DECIMAL(24, 8),
    net_asset DECIMAL(28, 6), total_netasset DECIMAL(28, 6), adj_nav DECIMAL(24, 8),
    updated_at TIMESTAMPTZ DEFAULT NOW(), created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, nav_date)
);
CREATE INDEX IF NOT EXISTS idx_fund_nav_nav_date ON fund_nav(nav_date);

CREATE TABLE IF NOT EXISTS fund_div (
    ts_code VARCHAR(20) NOT NULL, ann_date DATE NOT NULL, imp_anndate DATE,
    base_date DATE, div_proc VARCHAR(50), record_date DATE, ex_date DATE,
    pay_date DATE, earpay_date DATE, net_ex_date DATE, div_cash DECIMAL(24, 8),
    base_unit DECIMAL(28, 6), ear_distr DECIMAL(28, 6), ear_amount DECIMAL(28, 6),
    account_date DATE, base_year VARCHAR(20), updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(), PRIMARY KEY (ts_code, ann_date)
);
CREATE INDEX IF NOT EXISTS idx_fund_div_ann_date ON fund_div(ann_date);
CREATE INDEX IF NOT EXISTS idx_fund_div_ex_date ON fund_div(ex_date);

COMMENT ON TABLE fund_share IS 'Tushare 公募基金规模数据';
COMMENT ON TABLE fund_nav IS 'Tushare 公募基金净值数据';
COMMENT ON TABLE fund_div IS 'Tushare 公募基金分红数据';
