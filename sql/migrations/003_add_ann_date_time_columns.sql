-- Migration script to add ann_date_time and f_ann_date_time columns to existing financial tables
-- and backfill data from existing ann_date and f_ann_date columns.
-- 
-- Run this script against your existing database to migrate historical data.

-- =====================================================
-- fina_indicator table: Add ann_date_time column
-- =====================================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ann_date_time TIMESTAMPTZ;

-- Backfill ann_date_time from ann_date (format: YYYYMMDD)
UPDATE fina_indicator
SET ann_date_time = TO_TIMESTAMP(ann_date, 'YYYYMMDD')
WHERE ann_date IS NOT NULL
  AND ann_date ~ '^\d{8}$'
  AND ann_date_time IS NULL;

-- Create index
CREATE INDEX IF NOT EXISTS idx_fina_indicator_ann_date_time ON fina_indicator(ann_date_time);

-- =====================================================
-- cashflow table: Add ann_date_time and f_ann_date_time
-- =====================================================
ALTER TABLE cashflow ADD COLUMN IF NOT EXISTS ann_date_time TIMESTAMPTZ;
ALTER TABLE cashflow ADD COLUMN IF NOT EXISTS f_ann_date_time TIMESTAMPTZ;

-- Backfill ann_date_time
UPDATE cashflow
SET ann_date_time = TO_TIMESTAMP(ann_date, 'YYYYMMDD')
WHERE ann_date IS NOT NULL
  AND ann_date ~ '^\d{8}$'
  AND ann_date_time IS NULL;

-- Backfill f_ann_date_time
UPDATE cashflow
SET f_ann_date_time = TO_TIMESTAMP(f_ann_date, 'YYYYMMDD')
WHERE f_ann_date IS NOT NULL
  AND f_ann_date ~ '^\d{8}$'
  AND f_ann_date_time IS NULL;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_cashflow_ann_date_time ON cashflow(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_cashflow_f_ann_date_time ON cashflow(f_ann_date_time);

-- =====================================================
-- balancesheet table: Add ann_date_time and f_ann_date_time
-- =====================================================
ALTER TABLE balancesheet ADD COLUMN IF NOT EXISTS ann_date_time TIMESTAMPTZ;
ALTER TABLE balancesheet ADD COLUMN IF NOT EXISTS f_ann_date_time TIMESTAMPTZ;

-- Backfill ann_date_time
UPDATE balancesheet
SET ann_date_time = TO_TIMESTAMP(ann_date, 'YYYYMMDD')
WHERE ann_date IS NOT NULL
  AND ann_date ~ '^\d{8}$'
  AND ann_date_time IS NULL;

-- Backfill f_ann_date_time
UPDATE balancesheet
SET f_ann_date_time = TO_TIMESTAMP(f_ann_date, 'YYYYMMDD')
WHERE f_ann_date IS NOT NULL
  AND f_ann_date ~ '^\d{8}$'
  AND f_ann_date_time IS NULL;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_balancesheet_ann_date_time ON balancesheet(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_balancesheet_f_ann_date_time ON balancesheet(f_ann_date_time);

-- =====================================================
-- income table: Add ann_date_time and f_ann_date_time
-- =====================================================
ALTER TABLE income ADD COLUMN IF NOT EXISTS ann_date_time TIMESTAMPTZ;
ALTER TABLE income ADD COLUMN IF NOT EXISTS f_ann_date_time TIMESTAMPTZ;

-- Backfill ann_date_time
UPDATE income
SET ann_date_time = TO_TIMESTAMP(ann_date, 'YYYYMMDD')
WHERE ann_date IS NOT NULL
  AND ann_date ~ '^\d{8}$'
  AND ann_date_time IS NULL;

-- Backfill f_ann_date_time
UPDATE income
SET f_ann_date_time = TO_TIMESTAMP(f_ann_date, 'YYYYMMDD')
WHERE f_ann_date IS NOT NULL
  AND f_ann_date ~ '^\d{8}$'
  AND f_ann_date_time IS NULL;

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_income_ann_date_time ON income(ann_date_time);
CREATE INDEX IF NOT EXISTS idx_income_f_ann_date_time ON income(f_ann_date_time);

-- =====================================================
-- Verification: Check the migration results
-- =====================================================
SELECT 'fina_indicator' as table_name, 
       COUNT(*) as total_rows,
       COUNT(ann_date_time) as with_ann_date_time
FROM fina_indicator
UNION ALL
SELECT 'cashflow',
       COUNT(*),
       COUNT(ann_date_time)
FROM cashflow
UNION ALL
SELECT 'balancesheet',
       COUNT(*),
       COUNT(ann_date_time)
FROM balancesheet
UNION ALL
SELECT 'income',
       COUNT(*),
       COUNT(ann_date_time)
FROM income;
