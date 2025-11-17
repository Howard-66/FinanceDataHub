# 复权因子实施完成报告

## 📋 实施概览

复权因子功能已完全实施，包括数据获取、存储、查询和CLI命令支持。

## 🐛 Bug修复历史

在实施过程中遇到的兼容性问题及解决方案：

- **缺失字段错误** - Tushare返回数据缺少adj_factor等字段
  - 解决方案：在所有batch insert方法中添加字段验证和默认值处理

- **时区转换错误** - PostgreSQL timestamptz要求时区aware的datetime
  - 解决方案：实现`_normalize_datetime_for_db()`函数，将pandas Timestamp转换为UTC-aware datetime

- **TimescaleDB兼容性问题**（多次修复）
  - 问题1：columnstore未启用错误 → 移除压缩策略，只保留数据保留策略
  - 问题2：系统表`timescaledb_information.retention_policies`不存在 → 使用异常处理替代系统表查询
  - 问题3：视图列`total_chunks`、`data_nodes`不存在 → 简化视图，只使用基础列

## ✅ 已完成的功能

### 1. 数据获取层面
- ✅ **TushareProvider.get_adj_factor()** - 获取复权因子数据
- ✅ **智能路由支持** - 通过SmartRouter自动路由到tushare
- ✅ **数据格式标准化** - 统一返回DataFrame格式

### 2. 数据存储层面
- ✅ **数据库表结构** - `sql/init/004_create_adj_factor.sql`
  - 主键：(symbol, trade_date)
  - 索引：symbol、trade_date、复合索引
  - TimescaleDB超表支持
  - 数据保留策略：保留5年数据，自动清理旧数据
  - SQL脚本兼容性修复（解决TimescaleDB columnstore错误）
  - 多版本兼容性修复（解决系统表和列不存在问题）

- ✅ **DataOperations方法**：
  - `insert_adj_factor_batch()` - 批量插入复权因子
  - `get_adj_factor()` - 查询指定时间范围内的复权因子
  - 自动时区转换
  - NaT值处理

### 3. 数据更新层面
- ✅ **DataUpdater.update_adj_factor()** - 更新复权因子数据
  - 支持指定股票列表
  - 支持自定义日期范围
  - 默认获取最近1年数据
  - **增量更新逻辑** - 检查每只股票的最新数据日期，只获取更新的数据
  - 智能跳过 - 如果股票数据已最新，自动跳过避免重复获取

### 4. CLI命令层面
- ✅ **更新CLI支持**：
  ```bash
  # 更新所有股票复权因子（默认1年）
  fdh-cli update --frequency adj_factor

  # 更新指定股票复权因子
  fdh-cli update --frequency adj_factor --symbols 600519.SH,000858.SZ

  # 更新指定日期范围
  fdh-cli update --frequency adj_factor --start-date 2024-01-01 --end-date 2024-12-31
  ```

### 5. 配置层面
- ✅ **路由配置** - `sources.yml`中添加：
  ```yaml
  routing_strategy:
    stock:
      adj_factor:
        providers: [tushare]
        fallback: false
  ```

## 🏗️ 技术实现

### 数据库设计

```sql
CREATE TABLE adj_factor (
    symbol VARCHAR(20) NOT NULL,
    trade_date DATE NOT NULL,
    adj_factor DECIMAL(15, 8) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, trade_date)
);
```

**特性**：
- ✅ TimescaleDB超表支持
- ✅ 数据保留策略（5年自动清理）
- ✅ 复合主键避免重复
- ✅ 时间戳追踪（创建/更新）
- ✅ 版本兼容性（修复TimescaleDB columnstore错误和多版本系统表问题）

### 核心代码

**1. TushareProvider.get_adj_factor()**
```python
def get_adj_factor(
    self,
    symbol: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> pd.DataFrame:
    """
    获取复权因子
    返回：symbol, trade_date, adj_factor
    """
    df = self._call_api("adj_factor", **kwargs)
    # 列名映射和格式转换
    return df.sort_values("trade_date").reset_index(drop=True)
```

**2. DataOperations.insert_adj_factor_batch()**
```python
async def insert_adj_factor_batch(
    self, data: pd.DataFrame, batch_size: int = 1000
) -> int:
    """批量插入复权因子，自动处理时区和NaT值"""
    for record in records:
        for key, value in record.items():
            if pd.isna(value):
                record[key] = None
            elif key == "trade_date" or isinstance(value, pd.Timestamp):
                record[key] = _normalize_datetime_for_db(value)
    # 执行插入（支持ON CONFLICT更新）
```

**3. DataUpdater.update_adj_factor()**
```python
async def update_adj_factor(
    self,
    symbols: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> int:
    """更新复权因子数据（支持增量更新）"""
    # 获取股票列表
    # 确定全局日期范围（默认1年）
    # 遍历股票：
    #   1. 检查每只股票的最新复权因子日期
    #   2. 如果已有数据，从最新日期后一天开始获取
    #   3. 如果起始日期晚于结束日期，跳过该股票
    #   4. 获取复权因子数据并批量插入
    #   5. 记录跳过的股票数量
    # 返回更新的记录总数
```

## 🧪 测试验证

### 测试结果
```bash
$ uv run pytest -v
======================== 42 passed, 8 warnings in 4.53s ========================
```

### 测试覆盖
- ✅ Provider数据获取
- ✅ 数据库操作
- ✅ 数据更新器
- ✅ CLI命令
- ✅ 智能路由
- ✅ 错误处理

### CLI测试
```bash
$ fdh-cli update --help
--frequency    -f      TEXT  数据频率 (daily, minute_1, minute_5, basic,
                              adj_factor)
                              [default: daily]
```

## 📊 使用流程

### 1. 更新复权因子（增量更新）
```bash
# 更新所有股票复权因子（首次运行获取1年数据，后续增量更新）
fdh-cli update --frequency adj_factor

# 或指定股票
fdh-cli update --frequency adj_factor --symbols 600519.SH,000858.SZ

# 指定日期范围（覆盖默认的1年范围）
fdh-cli update --frequency adj_factor --start-date 2024-01-01 --end-date 2024-12-31
```

**增量更新机制**：
- ✅ 首次运行：获取所有股票最近1年的复权因子数据
- ✅ 后续运行：自动检测每只股票的最新数据日期，只获取更新的数据
- ✅ 智能跳过：如果股票数据已最新，自动跳过避免重复API调用
- ✅ 性能优化：避免对已更新股票的重复查询，节省API调用次数

### 2. 日线数据更新（可选：包含复权因子）
```bash
# 更新日线数据（复权因子可单独更新）
fdh-cli update --frequency daily

# 获取前复权日线数据（未来版本实现）
# fdh-cli update --frequency daily --adj qfq
```

### 3. 查看复权因子数据
```sql
-- 查看某只股票的复权因子
SELECT * FROM adj_factor
WHERE symbol = '600519.SH'
ORDER BY trade_date DESC
LIMIT 10;

-- 查看复权因子统计
SELECT
    COUNT(*) as total_records,
    COUNT(DISTINCT symbol) as total_symbols,
    MIN(trade_date) as earliest_date,
    MAX(trade_date) as latest_date
FROM adj_factor;
```

## 🔄 下一步优化（可选）

### 1. 复权计算实现
- 在数据访问层实现前复权/后复权计算
- 支持 `get_daily(adj='qfq')` 和 `get_daily(adj='hfq')`
- 缓存复权结果避免重复计算

### 2. 增量更新优化
- 检测分红送股事件
- 只在事件发生时更新复权因子
- 添加事件监听机制

### 3. 数据验证增强
- 验证复权因子连续性
- 检测异常值
- 自动报警

## 📝 注意事项

1. **API限频**：Tushare API有调用频率限制，注意控制更新频率
2. **数据范围**：复权因子变化不频繁，建议每季度或半年更新一次
3. **存储优化**：复权因子表使用TimescaleDB超表，支持数据保留策略（5年），自动清理旧数据
4. **时区处理**：所有时间戳自动转换为UTC存储

## 📁 相关文件

- `finance_data_hub/providers/tushare.py` - get_adj_factor方法
- `finance_data_hub/providers/schema.py` - AdjFactorSchema
- `finance_data_hub/database/operations.py` - insert_adj_factor_batch, get_adj_factor
- `finance_data_hub/update/updater.py` - update_adj_factor方法
- `finance_data_hub/cli/main.py` - CLI命令支持
- `sql/init/004_create_adj_factor.sql` - 数据库表结构
- `sources.yml` - 路由配置

## 🎯 总结

复权因子功能已完全实施，包括：
- ✅ 数据获取（Tushare API）
- ✅ 数据存储（TimescaleDB超表）
- ✅ 数据更新（智能增量更新）
- ✅ CLI命令支持
- ✅ 智能路由
- ✅ 性能优化（避免重复API调用）
- ✅ 所有测试通过

**系统现在支持完整的复权因子生命周期管理和增量更新！**

**新增特性**：
- ✅ **智能增量更新** - 自动检测最新数据，只获取更新的内容
- ✅ **性能优化** - 跳过已更新的股票，节省API调用
- ✅ **日志增强** - 显示跳过的股票数量，便于监控

---
**实施时间**: 2025-11-16
**实施状态**: ✅ 完成
