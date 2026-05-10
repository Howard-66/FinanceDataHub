-- Migration 022: add preclose to symbol_daily for HK adj_factor derivation

ALTER TABLE symbol_daily
ADD COLUMN IF NOT EXISTS preclose DECIMAL(20,6);

COMMENT ON COLUMN symbol_daily.preclose IS
'昨收价（主要用于港股 adj_factor 本地递推；A股可为空）';
