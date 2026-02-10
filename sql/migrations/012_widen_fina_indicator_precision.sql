-- ==========================================
-- 迁移: 扩展 fina_indicator 表字段精度
-- 将所有 DECIMAL(10,4)、DECIMAL(15,4)、DECIMAL(10,2) 统一为 DECIMAL(20,4)
-- 防止极端金融数据导致的 NumericValueOutOfRange 错误
-- ==========================================

-- 每股指标 DECIMAL(15,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN eps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN dt_eps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN total_revenue_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN revenue_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN capital_rese_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN surplus_rese_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN undist_profit_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN diluted2_eps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN bps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocfps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN retainedps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN cfps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ebit_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN fcff_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN fcfe_ps TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_eps TYPE DECIMAL(20,4);

-- 盈利比率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN netprofit_margin TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN grossprofit_margin TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN cogs_of_sales TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN expense_of_sales TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN profit_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN saleexp_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN adminexp_of_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN finaexp_of_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN impai_ttm TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN gc_of_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN op_of_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ebit_of_gr TYPE DECIMAL(20,4);

-- 收益率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN roe TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roe_waa TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roe_dt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roa TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN npta TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roic TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roe_yearly TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roa2_yearly TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roe_avg TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roa_yearly TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roa_dp TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN roic_yearly TYPE DECIMAL(20,4);

-- 利润结构比率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN opincome_of_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN investincome_of_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN n_op_profit_of_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN tax_to_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN dtprofit_to_profit TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN op_to_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN nop_to_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN profit_to_op TYPE DECIMAL(20,4);

-- 现金流比率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN salescash_to_or TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_or TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_opincome TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN capitalized_to_da TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_profit TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN cash_to_liqdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN cash_to_liqdebt_withinterest TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN op_to_liqdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN op_to_debt TYPE DECIMAL(20,4);

-- 偿债/资本结构比率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN debt_to_assets TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN assets_to_eqt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN dp_assets_to_eqt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ca_to_assets TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN nca_to_assets TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN tbassets_to_totalassets TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN int_to_talcap TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN eqt_to_talcapital TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN currentdebt_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN longdeb_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_shortdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN debt_to_eqt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN eqt_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN eqt_to_interestdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN tangibleasset_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN tangasset_to_intdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN tangibleasset_to_netdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_interestdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ocf_to_netdebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ebit_to_interest TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN longdebt_to_workingcapital TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN ebitda_to_debt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN total_fa_trun TYPE DECIMAL(20,4);

-- 营业周期 DECIMAL(10,2) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN turn_days TYPE DECIMAL(20,4);

-- 单季度比率 DECIMAL(10,4) -> DECIMAL(20,4)
ALTER TABLE fina_indicator ALTER COLUMN q_netprofit_margin TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_gsprofit_margin TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_exp_to_sales TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_profit_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_saleexp_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_adminexp_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_finaexp_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_impair_to_gr_ttm TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_gc_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_op_to_gr TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_roe TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_dt_roe TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_npta TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_opincome_to_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_investincome_to_ebt TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_dtprofit_to_profit TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_salescash_to_or TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_ocf_to_sales TYPE DECIMAL(20,4);
ALTER TABLE fina_indicator ALTER COLUMN q_ocf_to_or TYPE DECIMAL(20,4);

-- 验证
SELECT 'fina_indicator precision widening migration' as migration,
       count(*) as total_columns
FROM information_schema.columns
WHERE table_name = 'fina_indicator';
