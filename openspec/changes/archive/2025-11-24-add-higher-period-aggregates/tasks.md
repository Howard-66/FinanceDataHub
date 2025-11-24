# 任务：添加高周期数据聚合

## 概述
将高周期数据聚合的实现分解为可验证的增量工作项。

## 任务列表

### 1. 创建连续聚合的 SQL 脚本
**交付物**：`sql/init/006_create_continuous_aggregates.sql`

创建包含以下内容的 SQL 初始化脚本：
- [ ] symbol_weekly 连续聚合定义
- [ ] symbol_monthly 连续聚合定义
- [ ] daily_basic_weekly 连续聚合定义
- [ ] daily_basic_monthly 连续聚合定义
- [ ] 所有聚合的刷新策略
- [ ] 查询优化的索引
- [ ] 记录聚合逻辑的注释

**验证**：
```bash
# 对测试数据库运行 SQL 脚本
psql -f sql/init/006_create_continuous_aggregates.sql

# 验证聚合已创建
psql -c "SELECT view_name FROM timescaledb_information.continuous_aggregates;"
```

**依赖**：无（使用现有的 symbol_daily 和 daily_basic 表）

---

### 2. 更新数据库初始化器
**交付物**：修改后的 `finance_data_hub/database/init_db.py`

更新数据库初始化逻辑：
- [ ] 将 006_create_continuous_aggregates.sql 添加到初始化文件列表
- [ ] 更新表存在性检查以包含连续聚合
- [ ] 为聚合创建失败添加错误处理

**验证**：
```bash
fdh-cli init-db
# 应该创建所有聚合且无错误
```

**依赖**：任务 1

---

### 3. 为周线/月线查询添加 DataOperations 方法
**交付物**：修改后的 `finance_data_hub/database/operations.py`

实现新的查询方法：
- [ ] `async def get_weekly_data(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `async def get_monthly_data(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `async def get_daily_basic_weekly(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `async def get_daily_basic_monthly(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] 添加带有使用示例的文档字符串
- [ ] 优雅处理空结果

**验证**：
```python
# 单元测试
ops = DataOperations(db_manager)
df = await ops.get_weekly_data(['600519.SH'], '2024-01-01', '2024-12-31')
assert not df.empty
assert list(df.columns) == ['time', 'symbol', 'open', 'high', 'low', 'close', 'volume', 'amount', 'adj_factor']
```

**依赖**：任务 1, 2

---

### 4. 扩展 FinanceDataHub SDK 以支持高周期方法
**交付物**：修改后的 `finance_data_hub/sdk.py`（或等效 SDK 模块）

添加面向用户的 SDK 方法：
- [ ] `def get_weekly(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `def get_monthly(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `async def get_weekly_async(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `async def get_monthly_async(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `def get_daily_basic_weekly(symbols, start_date, end_date) -> pd.DataFrame`
- [ ] `def get_daily_basic_monthly(symbols, start_date, end_date) -> pd.DataFrame`

**验证**：
```python
from finance_data_hub import FinanceDataHub
fdh = FinanceDataHub(settings)
df = fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
assert len(df) == 52  # 大约 52 周
```

**依赖**：任务 3

---

### 5. 添加聚合管理的 CLI 命令
**交付物**：修改后的 `finance_data_hub/cli/main.py`

实现 CLI 命令：
- [ ] `fdh-cli refresh-aggregates --table <name> [--start DATE] [--end DATE]`
  - 连续聚合手动触发器
- [ ] `fdh-cli status --aggregates`
  - 显示聚合状态、刷新时间、存储大小

**验证**：
```bash
fdh-cli refresh-aggregates --table symbol_weekly --start 2024-01-01 --end 2024-12-31
# 应该无错误完成并记录刷新进度

fdh-cli status --aggregates
# 应该显示所有 4 个聚合及其元数据
```

**依赖**：任务 1, 2

---

### 6. 编写查询方法的单元测试
**交付物**：`tests/test_higher_period_queries.py`

创建全面的测试套件：
- [ ] 使用模拟数据测试周线查询
- [ ] 使用模拟数据测试月线查询
- [ ] 测试每日基础聚合
- [ ] 测试空结果处理
- [ ] 测试日期范围边界情况（部分周/月）
- [ ] 测试多符号查询

**验证**：
```bash
pytest tests/test_higher_period_queries.py -v
# 所有测试应该通过
```

**依赖**：任务 3, 4

---

### 7. 编写连续聚合的集成测试
**交付物**：`tests/integration/test_continuous_aggregates.py`

创建端到端测试：
- [ ] 测试从 SQL 脚本创建聚合
- [ ] 测试初始数据填充
- [ ] 测试插入后自动刷新
- [ ] 测试与 Pandas 重采样的数据准确性
- [ ] 测试性能基准（查询 < 100ms）

**验证**：
```bash
pytest tests/integration/test_continuous_aggregates.py -v --integration
# 所有集成测试通过
```

**依赖**：任务 1-4

---

### 8. 添加数据准确性验证脚本
**交付物**：`scripts/validate_aggregates.py`

创建验证脚本：
- [ ] 将 symbol_weekly 与 symbol_daily 的 Pandas 重采样进行比较
- [ ] 将 symbol_monthly 与 symbol_daily 的 Pandas 重采样进行比较
- [ ] 生成包含准确性指标的报告
- [ ] 标记 > 0.01% 容差的差异

**验证**：
```bash
python scripts/validate_aggregates.py --symbol 600519.SH --year 2024
# 应该输出 "所有聚合验证成功"
```

**依赖**：任务 1-4

---

### 9. 更新文档
**交付物**：更新后的 `README.md`、`CLAUDE.md`，新使用指南

文档更新：
- [ ] 在 README.md 中添加高周期数据部分
- [ ] 记录带示例的 SDK 方法
- [ ] 记录聚合管理的 CLI 命令
- [ ] 添加性能特性
- [ ] 添加刷新问题的故障排除指南
- [ ] 更新 CLAUDE.md 包含设计决策

**验证**：
- 手动审查文档清晰度
- 验证所有示例可执行

**依赖**：任务 1-5

---

### 10. 为现有数据库创建迁移指南
**交付物**：`docs/migration/add_continuous_aggregates.md`

创建迁移指南：
- [ ] 现有部署的分步说明
- [ ] SQL 脚本执行命令
- [ ] 初始数据填充命令
- [ ] 验证步骤
- [ ] 回滚程序（如果需要）
- [ ] 估算停机时间和存储影响

**验证**：
- 在类似生产的数据库副本上测试迁移
- 验证所有步骤成功完成

**依赖**：任务 1-9

---

## 任务依赖关系图

```
任务 1 (SQL 脚本)
  ├─→ 任务 2 (数据库初始化器)
  │     ├─→ 任务 3 (DataOps 方法)
  │     │     ├─→ 任务 4 (SDK 方法)
  │     │     │     ├─→ 任务 6 (单元测试)
  │     │     │     ├─→ 任务 7 (集成测试)
  │     │     │     ├─→ 任务 9 (文档)
  │     │     │     └─→ 任务 10 (迁移指南)
  │     │     └─→ 任务 8 (验证脚本)
  │     └─→ 任务 5 (CLI 命令)
  └─→ 任务 7 (集成测试)
```

## 并行工作机会

- **任务 6（单元测试）**可以与任务 4（SDK 方法）并行开发，使用模拟对象
- **任务 9（文档）**可以在任务 4-5 期间并行起草
- **任务 8（验证脚本）**可以在任务 3 之后开始

## 完成定义

每个任务在以下情况下完成：
1. 代码已编写并通过代码检查（black、flake8、mypy）
2. 测试已编写并通过
3. 代码已审查（自审或同伴审查）
4. 验证步骤执行成功
5. 相关文档已更新

## 估算工作量

| 任务 | 工作量 | 优先级 |
|------|--------|----------|
| 1 | 4h | 高 |
| 2 | 2h | 高 |
| 3 | 4h | 高 |
| 4 | 3h | 高 |
| 5 | 3h | 中 |
| 6 | 4h | 高 |
| 7 | 5h | 高 |
| 8 | 3h | 中 |
| 9 | 3h | 中 |
| 10 | 2h | 低 |

**总计**：约 33 小时
