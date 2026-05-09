-- Migration: add exchange column to asset_basic for multi-market filtering.

ALTER TABLE asset_basic
ADD COLUMN IF NOT EXISTS exchange VARCHAR(20);

UPDATE asset_basic
SET exchange = UPPER(SPLIT_PART(symbol, '.', 2))
WHERE (exchange IS NULL OR exchange = '')
  AND symbol LIKE '%.%';

CREATE INDEX IF NOT EXISTS idx_asset_basic_exchange ON asset_basic(exchange);

COMMENT ON COLUMN asset_basic.exchange IS '交易所代码：SH/SZ/BJ/HK等';
