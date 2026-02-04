-- Migration: 扩展 change_pct 字段精度以容纳早期历史数据
-- 问题：1990年代早期股票 IPO 首日涨跌幅可达 18430% ~ 38300%
-- 解决：将 DECIMAL(10,6) 扩展为 DECIMAL(16,6)，支持最大 9999999999.999999

-- 修改 symbol_daily 表的 change_pct 列精度
ALTER TABLE symbol_daily 
ALTER COLUMN change_pct TYPE DECIMAL(16,6);

-- 验证修改结果
-- SELECT column_name, data_type, numeric_precision, numeric_scale 
-- FROM information_schema.columns 
-- WHERE table_name = 'symbol_daily' AND column_name = 'change_pct';
