-- Migration: 031_create_fund_basic
-- Description: Persist all Tushare fund_basic output fields.

CREATE TABLE IF NOT EXISTS fund_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(200),
    management VARCHAR(200),
    custodian VARCHAR(200),
    fund_type VARCHAR(100),
    found_date DATE,
    due_date DATE,
    list_date DATE,
    issue_date DATE,
    delist_date DATE,
    issue_amount DECIMAL(20,6),
    m_fee DECIMAL(20,6),
    c_fee DECIMAL(20,6),
    duration_year DECIMAL(20,6),
    p_value DECIMAL(20,6),
    min_amount DECIMAL(20,6),
    exp_return DECIMAL(20,6),
    benchmark TEXT,
    status VARCHAR(10),
    invest_type VARCHAR(100),
    type VARCHAR(100),
    trustee VARCHAR(200),
    purc_startdate DATE,
    redm_startdate DATE,
    market VARCHAR(10),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fund_basic_market ON fund_basic(market);
CREATE INDEX IF NOT EXISTS idx_fund_basic_status ON fund_basic(status);
CREATE INDEX IF NOT EXISTS idx_fund_basic_fund_type ON fund_basic(fund_type);
CREATE INDEX IF NOT EXISTS idx_fund_basic_management ON fund_basic(management);

COMMENT ON TABLE fund_basic IS 'Tushare 公募基金基础信息表';
COMMENT ON COLUMN fund_basic.ts_code IS 'TS 基金代码';
COMMENT ON COLUMN fund_basic.market IS '交易市场：E 场内，O 场外';
COMMENT ON COLUMN fund_basic.status IS '存续状态：D 摘牌，I 发行，L 已上市';
