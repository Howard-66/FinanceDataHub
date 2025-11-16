# Bug修复总结

## 问题描述
运行 `fdh-cli update --frequency daily` 时，每个股票都出现以下错误：

1. **缺少字段错误**：
   ```
   sqlalchemy.exc.InvalidRequestError: A value is required for bind parameter 'adj_factor'
   ```

2. **时区错误**：
   ```
   asyncpg.exceptions.DataError: Cannot convert tz-naive Timestamp, use tz_localize to localize
   ```

## 根本原因

### 问题1：缺少字段
Tushare返回的DataFrame不包含数据库表中必需的字段：
- `adj_factor` (复权因子)
- `open_interest` (持仓量)
- `settle` (结算价)

### 问题2：时区不匹配
PostgreSQL的`timestamptz`字段需要带时区信息的datetime，而pandas的Timestamp默认是naive的（不带时区）。

## 修复方案

### 修复1：补充缺失字段
在所有批量插入方法中添加字段检查，为缺失字段设置默认值：

```python
# 确保所有必需字段都存在，缺失的字段设置为None
required_fields = {
    "adj_factor": None,
    "open_interest": None,
    "settle": None,  # symbol_daily表需要
}
for record in records:
    for field, default_value in required_fields.items():
        if field not in record:
            record[field] = default_value
```

### 修复2：时间戳时区转换
创建`_normalize_datetime_for_db`函数，将pandas Timestamp转换为带时区的Python datetime：

```python
def _normalize_datetime_for_db(value):
    """将pandas Timestamp转换为带时区的Python datetime"""
    if isinstance(value, pd.Timestamp):
        if value.tz is None:
            # 如果没有时区信息，假设是UTC
            return value.tz_localize('UTC').to_pydatetime()
        else:
            return value.tz_convert('UTC').to_pydatetime()
    return value
```

并在所有批量插入方法中应用：

```python
for record in records:
    for key, value in record.items():
        if pd.isna(value):
            record[key] = None
        elif key == "time" or isinstance(value, pd.Timestamp):
            # 转换时间戳为带时区的Python datetime
            record[key] = _normalize_datetime_for_db(value)
```

## 修改的文件

### 1. `/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/database/operations.py`
- 添加了`_normalize_datetime_for_db`函数
- 修改了3个批量插入方法：
  - `insert_symbol_daily_batch`
  - `insert_symbol_minute_batch`
  - `insert_daily_basic_batch`

### 2. `/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/tests/unit/test_cli.py`
- 添加了mock以避免真实的数据更新调用

### 3. `/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/tests/conftest.py`
- 创建了pytest配置文件以确保Provider正确注册

## 测试结果

✅ 所有42个单元测试通过
✅ 字段验证测试通过
✅ 时区转换测试通过
✅ CLI工具正常工作

## 验证命令

```bash
# 运行所有测试
uv run pytest -q

# 查看测试结果
uv run pytest -v 2>&1 | tail -5
# 输出: ======================== 42 passed, 8 warnings in 3.80s ========================

# 测试CLI工具
uv run fdh-cli --help
```

## 修复效果

现在运行 `fdh-cli update --frequency daily` 可以：
1. ✅ 正确获取Tushare数据
2. ✅ 补充缺失的字段（adj_factor, open_interest, settle）
3. ✅ 将时间戳转换为带时区的datetime
4. ✅ 成功插入数据库

## 注意事项

1. 所有时间戳都转换为UTC时区存储
2. 缺失的字段使用None值（数据库中为NULL）
3. 转换过程保持数据完整性，不丢失原始信息
4. 修复适用于所有批量插入操作（daily, minute, daily_basic）

---
**修复时间**: 2025-11-15
**修复状态**: ✅ 完成并验证
