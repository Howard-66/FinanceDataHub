-- ==========================================
-- 迁移: 添加 fina_indicator 缺失字段
-- 统一使用 DECIMAL(20,4) 精度
-- ==========================================

-- ==========================================
-- 周转率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS invturn_days DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.invturn_days IS '存货周转天数';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS arturn_days DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.arturn_days IS '应收账款周转天数';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS inv_turn DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.inv_turn IS '存货周转率';

-- ==========================================
-- 收益/折旧字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS valuechange_income DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.valuechange_income IS '价值变动净收益';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS interst_income DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.interst_income IS '利息费用';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS daa DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.daa IS '折旧与摊销';

-- ==========================================
-- 每股指标字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS retainedps DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.retainedps IS '每股留存收益';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS fcff_ps DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.fcff_ps IS '每股企业自由现金流量';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS fcfe_ps DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.fcfe_ps IS '每股股东自由现金流量';

-- ==========================================
-- 销售费用率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS cogs_of_sales DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.cogs_of_sales IS '销售成本率';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS expense_of_sales DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.expense_of_sales IS '销售期间费用率';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS saleexp_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.saleexp_to_gr IS '销售费用/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS adminexp_of_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.adminexp_of_gr IS '管理费用/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS finaexp_of_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.finaexp_of_gr IS '财务费用/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS impai_ttm DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.impai_ttm IS '资产减值损失/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS gc_of_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.gc_of_gr IS '营业总成本/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS op_of_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.op_of_gr IS '营业利润/营业总收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ebit_of_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ebit_of_gr IS '息税前利润/营业总收入';

-- ==========================================
-- 盈利能力/回报率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS npta DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.npta IS '总资产净利润';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS roa2_yearly DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.roa2_yearly IS '年化总资产报酬率';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS roe_avg DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.roe_avg IS '平均净资产收益率(增发条件)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS roa_yearly DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.roa_yearly IS '年化总资产净利率';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS roa_dp DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.roa_dp IS '总资产净利率(杜邦分析)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS roic_yearly DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.roic_yearly IS '年化投入资本回报率';

-- ==========================================
-- 利润结构比率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS opincome_of_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.opincome_of_ebt IS '经营活动净收益/利润总额';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS investincome_of_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.investincome_of_ebt IS '价值变动净收益/利润总额';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS n_op_profit_of_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.n_op_profit_of_ebt IS '营业外收支净额/利润总额';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS tax_to_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.tax_to_ebt IS '所得税/利润总额';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS dtprofit_to_profit DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.dtprofit_to_profit IS '扣除非经常损益后的净利润/净利润';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS nop_to_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.nop_to_ebt IS '非营业利润/利润总额';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS profit_to_op DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.profit_to_op IS '利润总额/营业收入';

-- ==========================================
-- 现金流相关比率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS salescash_to_or DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.salescash_to_or IS '销售商品提供劳务收到的现金/营业收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_or DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_or IS '经营活动产生的现金流量净额/营业收入';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_opincome DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_opincome IS '经营活动产生的现金流量净额/经营活动净收益';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS capitalized_to_da DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.capitalized_to_da IS '资本支出/折旧和摊销';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_profit DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_profit IS '经营活动产生的现金流量净额/营业利润';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS cash_to_liqdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.cash_to_liqdebt IS '货币资金/流动负债';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS cash_to_liqdebt_withinterest DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.cash_to_liqdebt_withinterest IS '货币资金/带息流动负债';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS op_to_liqdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.op_to_liqdebt IS '营业利润/流动负债';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS op_to_debt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.op_to_debt IS '营业利润/负债合计';

-- ==========================================
-- 偿债能力比率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS dp_assets_to_eqt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.dp_assets_to_eqt IS '权益乘数(杜邦分析)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_shortdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_shortdebt IS '经营活动产生的现金流量净额/流动负债';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS tangasset_to_intdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.tangasset_to_intdebt IS '有形资产/带息债务';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS tangibleasset_to_netdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.tangibleasset_to_netdebt IS '有形资产/净债务';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_interestdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_interestdebt IS '经营活动产生的现金流量净额/带息债务';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ocf_to_netdebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ocf_to_netdebt IS '经营活动产生的现金流量净额/净债务';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ebit_to_interest DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ebit_to_interest IS '已获利息倍数(EBIT/利息费用)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS longdebt_to_workingcapital DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.longdebt_to_workingcapital IS '长期债务与营运资金比率';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS ebitda_to_debt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.ebitda_to_debt IS '息税折旧摊销前利润/负债合计';

-- ==========================================
-- 固定资产周转率
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS total_fa_trun DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.total_fa_trun IS '固定资产合计周转率';

-- ==========================================
-- 单季度指标字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_investincome DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_investincome IS '价值变动单季度净收益';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_exp_to_sales DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_exp_to_sales IS '销售期间费用率(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_saleexp_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_saleexp_to_gr IS '销售费用/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_adminexp_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_adminexp_to_gr IS '管理费用/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_finaexp_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_finaexp_to_gr IS '财务费用/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_impair_to_gr_ttm DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_impair_to_gr_ttm IS '资产减值损失/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_gc_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_gc_to_gr IS '营业总成本/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_op_to_gr DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_op_to_gr IS '营业利润/营业总收入(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_roe DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_roe IS '净资产收益率(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_dt_roe DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_dt_roe IS '净资产单季度收益率(扣除非经常损益)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_npta DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_npta IS '总资产净利润(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_opincome_to_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_opincome_to_ebt IS '经营活动净收益/利润总额(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_investincome_to_ebt DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_investincome_to_ebt IS '价值变动净收益/利润总额(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_dtprofit_to_profit DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_dtprofit_to_profit IS '扣除非经常损益后的净利润/净利润(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_ocf_to_or DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_ocf_to_or IS '经营活动产生的现金流量净额/经营活动净收益(单季度)';

-- ==========================================
-- 增长率字段
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_gr_qoq DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_gr_qoq IS '营业总收入环比增长率(%)(单季度)';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS q_sales_qoq DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.q_sales_qoq IS '营业收入环比增长率(%)(单季度)';

-- ==========================================
-- 研发费用和更新标识
-- ==========================================
ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS rd_exp DECIMAL(20,4);
COMMENT ON COLUMN fina_indicator.rd_exp IS '研发费用';

ALTER TABLE fina_indicator ADD COLUMN IF NOT EXISTS update_flag VARCHAR(4);
COMMENT ON COLUMN fina_indicator.update_flag IS '更新标识';

-- 验证新增列
SELECT 'fina_indicator missing fields migration' as migration,
       count(*) as total_columns
FROM information_schema.columns
WHERE table_name = 'fina_indicator';
