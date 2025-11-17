-- 创建必需的数据库扩展
-- TimescaleDB 用于时间序列数据

-- 启用TimescaleDB扩展
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- 启用UUID扩展
CREATE EXTENSION IF NOT EXISTS "uuid-ossp" CASCADE;

-- 启用pg_trgm扩展用于文本搜索
CREATE EXTENSION IF NOT EXISTS pg_trgm CASCADE;

COMMENT ON EXTENSION timescaledb IS 'Time-series database extension for PostgreSQL';
COMMENT ON EXTENSION "uuid-ossp" IS 'Generate universally unique identifiers';
COMMENT ON EXTENSION pg_trgm IS 'Text similarity search and index support';
