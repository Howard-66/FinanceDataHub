-- Migration: 035_create_etf_basic
-- Description: Persist all Tushare etf_basic output fields.

CREATE TABLE IF NOT EXISTS etf_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    csname VARCHAR(200),
    extname VARCHAR(300),
    cname VARCHAR(500),
    index_code VARCHAR(20),
    index_name VARCHAR(500),
    setup_date DATE,
    list_date DATE,
    list_status VARCHAR(10),
    exchange VARCHAR(10),
    mgr_name VARCHAR(200),
    custod_name VARCHAR(300),
    mgt_fee DECIMAL(20,8),
    etf_type VARCHAR(100),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_etf_basic_index_code ON etf_basic(index_code);
CREATE INDEX IF NOT EXISTS idx_etf_basic_list_status ON etf_basic(list_status);
CREATE INDEX IF NOT EXISTS idx_etf_basic_exchange ON etf_basic(exchange);
CREATE INDEX IF NOT EXISTS idx_etf_basic_mgr_name ON etf_basic(mgr_name);

COMMENT ON TABLE etf_basic IS 'Tushare ETF基础信息表';
COMMENT ON COLUMN etf_basic.ts_code IS 'ETF交易代码';
COMMENT ON COLUMN etf_basic.list_status IS '存续状态：L上市 D退市 P待上市';
COMMENT ON COLUMN etf_basic.etf_type IS '基金投资通道类型：境内或QDII';
