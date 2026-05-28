-- 合并期货期限结构、跨期价差和展期收益率为单一快照表。
-- 只迁移库表结构，不迁移历史数据；重跑 fdh-cli update --asset-class future --dataset term_metrics 可重建数据。

DROP TABLE IF EXISTS futures.term_structure CASCADE;
DROP TABLE IF EXISTS futures.term_spread CASCADE;
DROP TABLE IF EXISTS futures.roll_yield CASCADE;

CREATE TABLE IF NOT EXISTS futures.term_metrics (
    time TIMESTAMPTZ NOT NULL,
    product_code VARCHAR(16) NOT NULL,
    exchange VARCHAR(16),
    flag DECIMAL(10,4),
    primary_contract VARCHAR(32),
    primary_contract_close DECIMAL(20,6),
    secondary_contract VARCHAR(32),
    secondary_contract_close DECIMAL(20,6),
    spread DECIMAL(20,6),
    days_to_primary_expiry INTEGER,
    days_between_expiry INTEGER,
    annualized_roll_yield DECIMAL(20,10),
    candidate_count INTEGER,
    source VARCHAR(32) DEFAULT 'preprocess',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (product_code, time)
);

SELECT create_hypertable(
    'futures.term_metrics',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);
