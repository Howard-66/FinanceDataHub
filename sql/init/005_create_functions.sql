-- 创建存储函数

-- 查找最新交易日期的函数
CREATE OR REPLACE FUNCTION get_latest_trading_date(symbol_param VARCHAR(20))
RETURNS TIMESTAMPTZ AS $$
DECLARE
    latest_date TIMESTAMPTZ;
BEGIN
    SELECT MAX(time) INTO latest_date
    FROM symbol_daily
    WHERE symbol = symbol_param;
    RETURN latest_date;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_latest_trading_date IS '获取指定股票的最新交易日期';

-- 批量插入或更新资产基本信息的函数
CREATE OR REPLACE FUNCTION upsert_asset_basic(
    p_symbol VARCHAR(20),
    p_name VARCHAR(100),
    p_market VARCHAR(20),
    p_area VARCHAR(50),
    p_industry VARCHAR(50),
    p_list_status VARCHAR(10),
    p_list_date DATE,
    p_delist_date DATE,
    p_is_hs VARCHAR(5)
)
RETURNS INTEGER AS $$
BEGIN
    INSERT INTO asset_basic (
        symbol, name, market, area, industry,
        list_status, list_date, delist_date, is_hs,
        updated_at
    ) VALUES (
        p_symbol, p_name, p_market, p_area, p_industry,
        p_list_status, p_list_date, p_delist_date, p_is_hs,
        NOW()
    )
    ON CONFLICT (symbol) DO UPDATE SET
        name = EXCLUDED.name,
        market = EXCLUDED.market,
        area = EXCLUDED.area,
        industry = EXCLUDED.industry,
        list_status = EXCLUDED.list_status,
        list_date = EXCLUDED.list_date,
        delist_date = EXCLUDED.delist_date,
        is_hs = EXCLUDED.is_hs,
        updated_at = NOW();
    RETURN 1;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION upsert_asset_basic IS '批量插入或更新资产基本信息';

-- 数据完整性检查函数
CREATE OR REPLACE FUNCTION check_data_integrity()
RETURNS TABLE(
    table_name TEXT,
    record_count BIGINT,
    latest_record TIMESTAMPTZ
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        'asset_basic'::TEXT,
        COUNT(*)::BIGINT,
        NULL::TIMESTAMPTZ
    FROM asset_basic
    UNION ALL
    SELECT
        'symbol_daily'::TEXT,
        COUNT(*)::BIGINT,
        MAX(time)::TIMESTAMPTZ
    FROM symbol_daily
    UNION ALL
    SELECT
        'symbol_minute'::TEXT,
        COUNT(*)::BIGINT,
        MAX(time)::TIMESTAMPTZ
    FROM symbol_minute
    UNION ALL
    SELECT
        'adj_factor'::TEXT,
        COUNT(*)::BIGINT,
        MAX(time)::TIMESTAMPTZ
    FROM adj_factor;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION check_data_integrity IS '检查各表数据完整性和最新记录';
