-- 创建视图和函数

-- 股票基础信息视图（仅显示在市股票）
CREATE OR REPLACE VIEW v_asset_basic_active AS
SELECT
    symbol,
    name,
    market,
    area,
    industry,
    list_date,
    is_hs
FROM asset_basic
WHERE list_status = 'L';

COMMENT ON VIEW v_asset_basic_active IS '活跃股票视图 - 仅显示在市交易股票';
