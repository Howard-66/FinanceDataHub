-- Tushare ETF 扩展数据。请由数据库管理员手动执行本迁移。
CREATE TABLE IF NOT EXISTS etf_index (
    ts_code VARCHAR(32) PRIMARY KEY, indx_name TEXT, indx_csname TEXT,
    pub_party_name TEXT, pub_date DATE, base_date DATE, bp NUMERIC(24,8),
    adj_circle TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_etf_index_pub_date ON etf_index(pub_date);

CREATE TABLE IF NOT EXISTS fund_daily (
    ts_code VARCHAR(32) NOT NULL, trade_date DATE NOT NULL,
    open NUMERIC(24,8), high NUMERIC(24,8), low NUMERIC(24,8), close NUMERIC(24,8),
    pre_close NUMERIC(24,8), change NUMERIC(24,8), pct_chg NUMERIC(24,8),
    vol NUMERIC(30,6), amount NUMERIC(30,6),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_fund_daily_trade_date ON fund_daily(trade_date);

CREATE TABLE IF NOT EXISTS fund_adj (
    ts_code VARCHAR(32) NOT NULL, trade_date DATE NOT NULL, adj_factor NUMERIC(24,10),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_fund_adj_trade_date ON fund_adj(trade_date);

CREATE TABLE IF NOT EXISTS etf_share_size (
    trade_date DATE NOT NULL, ts_code VARCHAR(32) NOT NULL, etf_name TEXT,
    total_share NUMERIC(30,6), total_size NUMERIC(30,6), nav NUMERIC(24,8),
    close NUMERIC(24,8), exchange VARCHAR(16),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
CREATE INDEX IF NOT EXISTS idx_etf_share_size_trade_date ON etf_share_size(trade_date);
CREATE INDEX IF NOT EXISTS idx_etf_share_size_exchange ON etf_share_size(exchange);

CREATE TABLE IF NOT EXISTS etf_sh_cons (
    trade_date DATE NOT NULL, ts_code VARCHAR(32) NOT NULL, con_code VARCHAR(32) NOT NULL,
    con_name TEXT, qty NUMERIC(30,6), sub_flag VARCHAR(32), cpr NUMERIC(24,8),
    rdr NUMERIC(24,8), sca NUMERIC(30,6), exchange VARCHAR(16),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, ts_code, con_code)
);
CREATE INDEX IF NOT EXISTS idx_etf_sh_cons_code_date ON etf_sh_cons(ts_code, trade_date DESC);

CREATE TABLE IF NOT EXISTS etf_sz_cons (
    trade_date DATE NOT NULL, ts_code VARCHAR(32) NOT NULL, con_code VARCHAR(32) NOT NULL,
    con_name TEXT, qty NUMERIC(30,6), sub_flag VARCHAR(32), cpr NUMERIC(24,8),
    rdr NUMERIC(24,8), sub_cc NUMERIC(30,6), red_cc NUMERIC(30,6), exchange VARCHAR(16),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, ts_code, con_code)
);
CREATE INDEX IF NOT EXISTS idx_etf_sz_cons_code_date ON etf_sz_cons(ts_code, trade_date DESC);

CREATE TABLE IF NOT EXISTS idx_anns (
    ann_date DATE NOT NULL, title TEXT NOT NULL, url TEXT, source VARCHAR(128) NOT NULL,
    type VARCHAR(128), updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(), PRIMARY KEY (ann_date, source, title)
);
CREATE INDEX IF NOT EXISTS idx_idx_anns_source_date ON idx_anns(source, ann_date DESC);

COMMENT ON TABLE etf_index IS 'Tushare ETF基准指数列表';
COMMENT ON TABLE fund_daily IS 'Tushare ETF日线行情';
COMMENT ON TABLE fund_adj IS 'Tushare 基金复权因子';
COMMENT ON TABLE etf_share_size IS 'Tushare ETF份额规模';
COMMENT ON TABLE etf_sh_cons IS 'Tushare ETF每日持仓组合（沪市）';
COMMENT ON TABLE etf_sz_cons IS 'Tushare ETF每日持仓组合（深市）';
COMMENT ON TABLE idx_anns IS 'Tushare 指数公告';
