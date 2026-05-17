-- Migration: simplify futures inventory receipt to product/date aggregate inventory.
--
-- The AKShare inventory paths used here persist aggregate inventory by product
-- and date. The old detail columns are not populated reliably and remain NULL
-- in normal updates, so remove them from the canonical schema.

ALTER TABLE futures.inventory_receipt
    DROP COLUMN IF EXISTS exchange,
    DROP COLUMN IF EXISTS receipt,
    DROP COLUMN IF EXISTS warehouse,
    DROP COLUMN IF EXISTS region;

COMMENT ON TABLE futures.inventory_receipt IS '期货库存数据表 - 品种日期聚合库存';
COMMENT ON COLUMN futures.inventory_receipt.time IS '日期';
COMMENT ON COLUMN futures.inventory_receipt.product_code IS '品种代码';
COMMENT ON COLUMN futures.inventory_receipt.inventory IS '库存';
COMMENT ON COLUMN futures.inventory_receipt.source IS '数据源';
