-- Standard Tushare moneyflow is the only individual-flow source used in the
-- PIT mainline factor layer.  THS/DC feeds are intentionally not created here.
BEGIN;

CREATE TABLE IF NOT EXISTS moneyflow (
    ts_code VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    buy_sm_vol NUMERIC(30,6), buy_sm_amount NUMERIC(30,6),
    sell_sm_vol NUMERIC(30,6), sell_sm_amount NUMERIC(30,6),
    buy_md_vol NUMERIC(30,6), buy_md_amount NUMERIC(30,6),
    sell_md_vol NUMERIC(30,6), sell_md_amount NUMERIC(30,6),
    buy_lg_vol NUMERIC(30,6), buy_lg_amount NUMERIC(30,6),
    sell_lg_vol NUMERIC(30,6), sell_lg_amount NUMERIC(30,6),
    buy_elg_vol NUMERIC(30,6), buy_elg_amount NUMERIC(30,6),
    sell_elg_vol NUMERIC(30,6), sell_elg_amount NUMERIC(30,6),
    net_mf_vol NUMERIC(30,6), net_mf_amount NUMERIC(30,6),
    source VARCHAR(32) NOT NULL DEFAULT 'tushare',
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ts_code, trade_date)
);
SELECT create_hypertable('moneyflow', 'trade_date', if_not_exists => TRUE, migrate_data => FALSE);

ALTER TABLE processed_mainline_stock_daily
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_ratio NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_large_net_amount NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_large_net_ratio NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_5d NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_20d NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_available BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE processed_mainline_industry_daily
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_ratio NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_large_net_amount NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_large_net_ratio NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_5d NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_net_amount_20d NUMERIC(30,6),
    ADD COLUMN IF NOT EXISTS moneyflow_positive_stock_ratio NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_top_stock_contribution NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_coverage NUMERIC(24,10),
    ADD COLUMN IF NOT EXISTS moneyflow_available BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE processed_mainline_market_daily
    ADD COLUMN IF NOT EXISTS northbound_available BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS northbound_available_from DATE;

COMMENT ON TABLE moneyflow IS 'Tushare 标准个股主动买卖资金流；主线策略唯一正式个股资金流原始来源';
COMMENT ON COLUMN processed_mainline_industry_daily.moneyflow_available IS '资金流为增强候选因子，不作为主线快照阻断条件';
COMMIT;
