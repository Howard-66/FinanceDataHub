-- 复权因子表
-- 创建复权因子表，用于存储股票的复权因子数据

CREATE TABLE IF NOT EXISTS adj_factor (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(15, 8) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_adj_factor_symbol ON adj_factor(symbol);
CREATE INDEX IF NOT EXISTS idx_adj_factor_date ON adj_factor(trade_date);
CREATE INDEX IF NOT EXISTS idx_adj_factor_symbol_date ON adj_factor(symbol, trade_date);

-- 创建更新时间戳触发器函数
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 创建触发器
DROP TRIGGER IF EXISTS update_adj_factor_updated_at ON adj_factor;
CREATE TRIGGER update_adj_factor_updated_at
    BEFORE UPDATE ON adj_factor
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 将表转换为TimescaleDB超表
-- 注意：TimescaleDB 2.0+ 的语法
SELECT create_hypertable('adj_factor', 'trade_date', if_not_exists => TRUE);

-- 设置数据保留策略（保留5年数据）
-- 注意：使用异常处理兼容不同TimescaleDB版本
DO $$
BEGIN
    -- 尝试添加保留策略，如果已存在会抛出异常
    PERFORM add_retention_policy('adj_factor', INTERVAL '5 years');
EXCEPTION
    WHEN duplicate_object THEN
        -- 策略已存在，忽略错误
        NULL;
END
$$;

-- 创建基本信息视图（只使用基础列，兼容所有版本）
CREATE OR REPLACE VIEW adj_factor_info AS
SELECT
    hypertable_name,
    compression_enabled
FROM
    timescaledb_information.hypertables
WHERE
    hypertable_name = 'adj_factor';

-- 注释
COMMENT ON TABLE adj_factor IS '复权因子表 - 存储股票的复权因子数据，用于前复权和后复权计算';
COMMENT ON COLUMN adj_factor.symbol IS '股票代码（如：600519.SH）';
COMMENT ON COLUMN adj_factor.trade_date IS '交易日期';
COMMENT ON COLUMN adj_factor.adj_factor IS '复权因子';
COMMENT ON COLUMN adj_factor.created_at IS '创建时间';
COMMENT ON COLUMN adj_factor.updated_at IS '更新时间';
