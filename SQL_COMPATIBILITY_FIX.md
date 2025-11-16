# SQL兼容性修复报告

## 🎯 修复目标

解决TimescaleDB多版本兼容性问题，确保复权因子表的数据库脚本在不同版本下都能正常运行。

## 🐛 遇到的问题

### 问题1: columnstore压缩策略错误

**错误信息**:
```
SQL Error [0A000]: ERROR: columnstore not enabled on hypertable "adj_factor"
Hint: Enable columnstore before adding a columnstore policy.
```

**根本原因**:
TimescaleDB的压缩策略默认使用columnstore，但复权因子表作为小表不需要复杂的压缩功能。

**解决方案**:
移除压缩策略，只保留数据保留策略。

---

### 问题2: 系统表不存在

**错误信息**:
```
SQL Error [42P01]: ERROR: relation "timescaledb_information.retention_policies" does not exist
```

**根本原因**:
不同TimescaleDB版本的系统表结构不同，某些版本可能没有这个表。

**解决方案**:
使用异常处理替代系统表查询：
```sql
DO $$
BEGIN
    PERFORM add_retention_policy('adj_factor', INTERVAL '5 years');
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END
$$;
```

---

### 问题3: 视图列不存在

**错误信息**:
```
SQL Error [42703]: ERROR: column ht.total_chunks does not exist
```

**根本原因**:
不同版本TimescaleDB的`timescaledb_information.hypertables`视图列不同。

**解决方案**:
简化视图，只使用最基础的列：
```sql
CREATE OR REPLACE VIEW adj_factor_info AS
SELECT
    hypertable_name,
    compression_enabled
FROM
    timescaledb_information.hypertables
WHERE
    hypertable_name = 'adj_factor';
```

## ✅ 最终修复方案

### 数据库表结构 (sql/init/004_create_adj_factor.sql)

```sql
-- 1. 创建基础表
CREATE TABLE IF NOT EXISTS adj_factor (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(15, 8) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);

-- 2. 创建索引
CREATE INDEX IF NOT EXISTS idx_adj_factor_symbol ON adj_factor(symbol);
CREATE INDEX IF NOT EXISTS idx_adj_factor_date ON adj_factor(trade_date);
CREATE INDEX IF NOT EXISTS idx_adj_factor_symbol_date ON adj_factor(symbol, trade_date);

-- 3. 创建触发器
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_adj_factor_updated_at ON adj_factor;
CREATE TRIGGER update_adj_factor_updated_at
    BEFORE UPDATE ON adj_factor
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- 4. 转换为TimescaleDB超表
SELECT create_hypertable('adj_factor', 'trade_date', if_not_exists => TRUE);

-- 5. 设置数据保留策略（兼容多版本）
DO $$
BEGIN
    PERFORM add_retention_policy('adj_factor', INTERVAL '5 years');
EXCEPTION
    WHEN duplicate_object THEN
        NULL;
END
$$;

-- 6. 创建基本信息视图（兼容多版本）
CREATE OR REPLACE VIEW adj_factor_info AS
SELECT
    hypertable_name,
    compression_enabled
FROM
    timescaledb_information.hypertables
WHERE
    hypertable_name = 'adj_factor';

-- 7. 添加注释
COMMENT ON TABLE adj_factor IS '复权因子表 - 存储股票的复权因子数据，用于前复权和后复权计算';
COMMENT ON COLUMN adj_factor.symbol IS '股票代码（如：600519.SH）';
COMMENT ON COLUMN adj_factor.trade_date IS '交易日期';
COMMENT ON COLUMN adj_factor.adj_factor IS '复权因子';
COMMENT ON COLUMN adj_factor.created_at IS '创建时间';
COMMENT ON COLUMN adj_factor.updated_at IS '更新时间';
```

## 🎯 兼容性策略

### 1. 异常处理优于系统表查询
- **使用场景**: 策略创建、对象检查
- **原因**: 不同版本系统表结构差异大，异常处理更可靠

### 2. 基础列优于高级列
- **使用场景**: 视图创建、信息查询
- **原因**: 高版本新增列在低版本不存在，使用基础列保证兼容性

### 3. 可选参数使用 if_not_exists
- **使用场景**: 超表创建、索引创建
- **原因**: 避免重复创建错误

## 📊 验证结果

- ✅ 所有单元测试通过 (42 passed)
- ✅ Python代码与数据库操作正常
- ✅ CLI命令可用：`fdh-cli update --frequency adj_factor`
- ✅ 智能路由正常工作
- ✅ 批量插入和查询功能正常

## 🔗 相关文件

- `sql/init/004_create_adj_factor.sql` - 最终修复的数据库脚本
- `finance_data_hub/database/operations.py` - 数据库操作层（包含时区转换）
- `finance_data_hub/providers/tushare.py` - 数据提供层
- `ADJ_FACTOR_IMPLEMENTATION.md` - 完整实施文档

## 💡 经验总结

1. **数据库兼容性**: 优先使用异常处理，避免依赖版本特定的系统表
2. **简化设计**: 对小表（如复权因子）采用轻量级设计，避免过度优化
3. **向后兼容**: 使用基础列和通用语法，确保脚本能在旧版本上运行
4. **渐进式优化**: 先实现基础功能，再根据实际需求添加高级特性

---

**修复时间**: 2025-11-16
**修复状态**: ✅ 完成
**测试状态**: ✅ 42/42 测试通过