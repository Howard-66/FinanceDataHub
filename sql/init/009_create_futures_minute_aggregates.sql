-- 期货分钟线：1m 原始表与高周期连续聚合

CREATE TABLE IF NOT EXISTS futures.minute_1m (
    time TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    product_code VARCHAR(16),
    exchange VARCHAR(16),
    open DECIMAL(20,6),
    high DECIMAL(20,6),
    low DECIMAL(20,6),
    close DECIMAL(20,6),
    volume BIGINT,
    amount DECIMAL(30,6),
    open_interest DECIMAL(30,6),
    source VARCHAR(32),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, time)
);

SELECT create_hypertable(
    'futures.minute_1m',
    'time',
    if_not_exists => TRUE,
    chunk_time_interval => INTERVAL '1 week'
);

CREATE INDEX IF NOT EXISTS idx_futures_minute_1m_symbol_time
ON futures.minute_1m(symbol, time DESC);

CREATE INDEX IF NOT EXISTS idx_futures_minute_1m_product_time
ON futures.minute_1m(product_code, time DESC);

-- 兼容旧表：如果 futures.minute 中已有 1m 数据，初始化时回填到新原始表。
INSERT INTO futures.minute_1m (
    time, symbol, product_code, exchange, open, high, low, close,
    volume, amount, open_interest, source, created_at, updated_at
)
SELECT
    time, symbol, product_code, exchange, open, high, low, close,
    volume, amount, open_interest, source, created_at, updated_at
FROM futures.minute
WHERE frequency = '1m'
ON CONFLICT (symbol, time) DO UPDATE SET
    product_code = EXCLUDED.product_code,
    exchange = EXCLUDED.exchange,
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    amount = EXCLUDED.amount,
    open_interest = EXCLUDED.open_interest,
    source = EXCLUDED.source,
    updated_at = NOW();

CREATE MATERIALIZED VIEW IF NOT EXISTS futures.minute_5m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '5 minutes', time) AS time,
    symbol,
    '5m'::VARCHAR(8) AS frequency,
    first(product_code, time) AS product_code,
    first(exchange, time) AS exchange,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume)::BIGINT AS volume,
    sum(amount) AS amount,
    last(open_interest, time) AS open_interest,
    first(source, time) AS source
FROM futures.minute_1m
GROUP BY time_bucket(INTERVAL '5 minutes', time), symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS futures.minute_15m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '15 minutes', time) AS time,
    symbol,
    '15m'::VARCHAR(8) AS frequency,
    first(product_code, time) AS product_code,
    first(exchange, time) AS exchange,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume)::BIGINT AS volume,
    sum(amount) AS amount,
    last(open_interest, time) AS open_interest,
    first(source, time) AS source
FROM futures.minute_1m
GROUP BY time_bucket(INTERVAL '15 minutes', time), symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS futures.minute_30m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '30 minutes', time) AS time,
    symbol,
    '30m'::VARCHAR(8) AS frequency,
    first(product_code, time) AS product_code,
    first(exchange, time) AS exchange,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume)::BIGINT AS volume,
    sum(amount) AS amount,
    last(open_interest, time) AS open_interest,
    first(source, time) AS source
FROM futures.minute_1m
GROUP BY time_bucket(INTERVAL '30 minutes', time), symbol
WITH NO DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS futures.minute_60m
WITH (timescaledb.continuous) AS
SELECT
    time_bucket(INTERVAL '1 hour', time) AS time,
    symbol,
    '60m'::VARCHAR(8) AS frequency,
    first(product_code, time) AS product_code,
    first(exchange, time) AS exchange,
    first(open, time) AS open,
    max(high) AS high,
    min(low) AS low,
    last(close, time) AS close,
    sum(volume)::BIGINT AS volume,
    sum(amount) AS amount,
    last(open_interest, time) AS open_interest,
    first(source, time) AS source
FROM futures.minute_1m
GROUP BY time_bucket(INTERVAL '1 hour', time), symbol
WITH NO DATA;

CREATE INDEX IF NOT EXISTS idx_futures_minute_5m_symbol_time
ON futures.minute_5m(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_minute_15m_symbol_time
ON futures.minute_15m(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_minute_30m_symbol_time
ON futures.minute_30m(symbol, time DESC);
CREATE INDEX IF NOT EXISTS idx_futures_minute_60m_symbol_time
ON futures.minute_60m(symbol, time DESC);

SELECT add_continuous_aggregate_policy(
    'futures.minute_5m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '5 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'futures.minute_15m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '15 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'futures.minute_30m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '30 minutes',
    if_not_exists => TRUE
);

SELECT add_continuous_aggregate_policy(
    'futures.minute_60m',
    start_offset => INTERVAL '7 days',
    end_offset => INTERVAL '1 minute',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

COMMENT ON TABLE futures.minute_1m IS '期货 1 分钟原始行情表';
COMMENT ON MATERIALIZED VIEW futures.minute_5m IS '期货 5 分钟行情连续聚合';
COMMENT ON MATERIALIZED VIEW futures.minute_15m IS '期货 15 分钟行情连续聚合';
COMMENT ON MATERIALIZED VIEW futures.minute_30m IS '期货 30 分钟行情连续聚合';
COMMENT ON MATERIALIZED VIEW futures.minute_60m IS '期货 60 分钟行情连续聚合';
