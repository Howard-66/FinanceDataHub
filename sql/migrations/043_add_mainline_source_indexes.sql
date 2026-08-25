-- Source-side indexes for the bulk mainline build. CONCURRENTLY keeps raw
-- update jobs readable; this file must not run inside a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_index_weight_code_date_constituent
  ON index_weight(index_code,trade_date,con_code) INCLUDE(weight);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_quarterly_fund_available_lookup
  ON processed_fundamental_quality(ts_code,f_ann_date_time,ann_date_time,end_date_time DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fina_indicator_ann_time_lookup
  ON fina_indicator(ts_code,ann_date_time,end_date_time DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fina_indicator_ann_text_lookup
  ON fina_indicator(ts_code,ann_date,end_date_time DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_stock_dividend_code_ann
  ON stock_dividend(ts_code,ann_date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mainline_repurchase_code_ann
  ON stock_repurchase(ts_code,ann_date);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_fund_portfolio_period_symbol_cover
  ON fund_portfolio(end_date,symbol) INCLUDE(ts_code,ann_date,mkv,stk_float_ratio,updated_at);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trade_cal_next_open
  ON trade_cal(exchange,is_open,cal_date);
