-- Migration: 034_create_fund_benchmark_portfolio
-- Description: Persist all Tushare mkt_idx_bmk and fund_portfolio output fields.

CREATE TABLE IF NOT EXISTS mkt_idx_bmk (
    ts_code VARCHAR(20) PRIMARY KEY,
    symbol VARCHAR(20),
    name VARCHAR(200),
    fullname VARCHAR(500),
    bmk_level VARCHAR(50),
    bmk_type VARCHAR(100),
    bmk_src VARCHAR(200),
    idx_type VARCHAR(100),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mkt_idx_bmk_level ON mkt_idx_bmk(bmk_level);
CREATE INDEX IF NOT EXISTS idx_mkt_idx_bmk_type ON mkt_idx_bmk(bmk_type);
COMMENT ON TABLE mkt_idx_bmk IS 'Tushare ETF业绩比较基准库';

CREATE TABLE IF NOT EXISTS fund_portfolio (
    ts_code VARCHAR(20) NOT NULL,
    ann_date DATE NOT NULL,
    end_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    mkv DECIMAL(24,6),
    amount DECIMAL(24,6),
    stk_mkv_ratio DECIMAL(20,6),
    stk_float_ratio DECIMAL(20,6),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (ts_code, ann_date, end_date, symbol)
);
CREATE INDEX IF NOT EXISTS idx_fund_portfolio_symbol_end_date ON fund_portfolio(symbol, end_date);
CREATE INDEX IF NOT EXISTS idx_fund_portfolio_end_date ON fund_portfolio(end_date);
COMMENT ON TABLE fund_portfolio IS 'Tushare 公募基金季度持仓';
