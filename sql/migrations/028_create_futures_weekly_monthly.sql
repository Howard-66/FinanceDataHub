-- Migration 028: add futures weekly/monthly raw K-line tables

CREATE SCHEMA IF NOT EXISTS futures;

CREATE TABLE IF NOT EXISTS futures.weekly (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    pre_close DECIMAL(20,6),
    change DECIMAL(20,6),
    pct_chg DECIMAL(16,6),
    volume BIGINT,
    amount DECIMAL(30,6),
    open_interest DECIMAL(30,6),
    source VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.weekly',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

CREATE INDEX IF NOT EXISTS idx_futures_weekly_product_time ON futures.weekly(product_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_weekly_exchange_time ON futures.weekly(exchange, time DESC);

CREATE TABLE IF NOT EXISTS futures.monthly (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    pre_close DECIMAL(20,6),
    change DECIMAL(20,6),
    pct_chg DECIMAL(16,6),
    volume BIGINT,
    amount DECIMAL(30,6),
    open_interest DECIMAL(30,6),
    source VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.monthly',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '5 years'
);

CREATE INDEX IF NOT EXISTS idx_futures_monthly_product_time ON futures.monthly(product_code, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_monthly_exchange_time ON futures.monthly(exchange, time DESC);
