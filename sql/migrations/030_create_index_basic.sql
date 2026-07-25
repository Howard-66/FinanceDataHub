-- Migration: 030_create_index_basic
-- Description: Persist Tushare index_basic metadata.

CREATE TABLE IF NOT EXISTS index_basic (
    ts_code VARCHAR(20) PRIMARY KEY,
    name VARCHAR(100),
    fullname TEXT,
    market VARCHAR(20),
    publisher VARCHAR(100),
    index_type VARCHAR(100),
    category VARCHAR(100),
    base_date DATE,
    base_point DECIMAL(20,6),
    list_date DATE,
    weight_rule TEXT,
    description TEXT,
    exp_date DATE,
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_index_basic_market ON index_basic(market);
CREATE INDEX IF NOT EXISTS idx_index_basic_publisher ON index_basic(publisher);
CREATE INDEX IF NOT EXISTS idx_index_basic_category ON index_basic(category);

COMMENT ON TABLE index_basic IS 'Tushare 指数基本信息表';
COMMENT ON COLUMN index_basic.ts_code IS 'TS 指数代码';
COMMENT ON COLUMN index_basic.base_date IS '基期';
COMMENT ON COLUMN index_basic.base_point IS '基点';
COMMENT ON COLUMN index_basic.list_date IS '发布日期';
COMMENT ON COLUMN index_basic.exp_date IS '终止日期';
