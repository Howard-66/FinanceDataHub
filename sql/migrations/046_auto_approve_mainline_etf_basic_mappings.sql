-- Promote the deterministic ETF catalogue mappings used by AlphaLine v3.
--
-- Tushare etf_basic supplies both the current benchmark index code and the
-- fund's listing/setup date.  For an ETF with both fields this is a sufficient
-- default mapping for v3.  The automatic source remains visible as medium
-- confidence, so an operator can replace it with a dated disclosure if the
-- fund later changes its tracking benchmark.
BEGIN;

UPDATE mainline_etf_benchmark_history h
SET benchmark_index_code = b.index_code,
    effective_from_date = COALESCE(b.list_date, b.setup_date),
    mapping_status = 'mapped',
    source_name = 'etf_basic_auto',
    evidence_reference = 'Automatically approved from etf_basic.index_code and listing/setup date',
    confidence = 'medium',
    review_status = 'approved',
    reviewed_at = NOW(),
    reviewed_by = 'system:etf_basic_auto',
    updated_at = NOW()
FROM etf_basic b
WHERE h.ts_code = b.ts_code
  AND h.mapping_status = 'mapping_pending'
  AND h.review_status = 'pending'
  AND b.index_code IS NOT NULL
  AND COALESCE(b.list_date, b.setup_date) IS NOT NULL;

COMMIT;
