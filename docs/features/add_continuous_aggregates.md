# 添加连续聚合的迁移指南

## 概述

本指南说明如何为现有的 FinanceDataHub 数据库添加连续聚合功能，该功能提供预计算的高周期（周线、月线）数据聚合，用于高效的分析和回测查询。

**预计完成时间**: 5-15 分钟
**需要停机时间**: 0-1 分钟（最小化）
**额外存储开销**: 约 5-10%

---

## 迁移前准备

### 1. 检查当前数据库状态

```bash
# 连接到数据库
psql "$DATABASE_URL"

# 检查现有表
\dt

# 检查 TimescaleDB 扩展是否已安装
SELECT extname FROM pg_extension WHERE extname = 'timescaledb';
```

### 2. 备份数据库（推荐）

```bash
# 全量备份
pg_dump "$DATABASE_URL" > backup_$(date +%Y%m%d_%H%M%S).sql

# 或仅备份核心表
pg_dump -t symbol_daily -t daily_basic -t adj_factor "$DATABASE_URL" > backup_core_tables_$(date +%Y%m%d_%H%M%S).sql
```

### 3. 确认数据完整性

```sql
-- 检查核心表的数据量
SELECT 'symbol_daily' as table_name, COUNT(*) as row_count FROM symbol_daily
UNION ALL
SELECT 'daily_basic' as table_name, COUNT(*) as row_count FROM daily_basic
UNION ALL
SELECT 'adj_factor' as table_name, COUNT(*) as row_count FROM adj_factor;
```

---

## 执行迁移

### 步骤 1: 确认数据库版本

连续聚合需要 TimescaleDB 2.0+ 版本。

```sql
-- 检查 TimescaleDB 版本
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
```

**要求**: TimescaleDB >= 2.0.0

### 步骤 2: 运行迁移脚本

```bash
# 执行连续聚合创建脚本
psql "$DATABASE_URL" -f sql/init/006_create_continuous_aggregates.sql
```

**期望输出**:
```
NOTICE: 成功创建 4 个连续聚合视图
```

### 步骤 3: 验证聚合创建

```sql
-- 检查聚合视图
SELECT view_name, view_owner
FROM timescaledb_information.continuous_aggregates
WHERE view_name IN (
  'symbol_weekly',
  'symbol_monthly',
  'daily_basic_weekly',
  'daily_basic_monthly',
  'adj_factor_weekly',
  'adj_factor_monthly'
)
ORDER BY view_name;
```

**预期结果**: 6 个聚合视图全部显示

### 步骤 4: 初始化聚合数据

由于连续聚合是自动维护的，需要手动触发初始数据填充：

```sql
-- 填充所有聚合的历史数据（根据数据量可能需要几分钟）
CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL);
CALL refresh_continuous_aggregate('symbol_monthly', NULL, NULL);
CALL refresh_continuous_aggregate('daily_basic_weekly', NULL, NULL);
CALL refresh_continuous_aggregate('daily_basic_monthly', NULL, NULL);
CALL refresh_continuous_aggregate('adj_factor_weekly', NULL, NULL);
CALL refresh_continuous_aggregate('adj_factor_monthly', NULL, NULL);
```

**验证填充状态**:
```sql
-- 检查聚合数据量
SELECT 'symbol_weekly' as aggregate, COUNT(*) as rows FROM symbol_weekly
UNION ALL
SELECT 'symbol_monthly' as aggregate, COUNT(*) as rows FROM symbol_monthly
UNION ALL
SELECT 'daily_basic_weekly' as aggregate, COUNT(*) as rows FROM daily_basic_weekly
UNION ALL
SELECT 'daily_basic_monthly' as aggregate, COUNT(*) as rows FROM daily_basic_monthly
UNION ALL
SELECT 'adj_factor_weekly' as aggregate, COUNT(*) as rows FROM adj_factor_weekly
UNION ALL
SELECT 'adj_factor_monthly' as aggregate, COUNT(*) as rows FROM adj_factor_monthly;
```

### 步骤 5: 验证刷新策略

```sql
-- 检查自动刷新策略
SELECT cap.view_name, cap.refresh_lag, cap.end_offset, cap.schedule_interval
FROM timescaledb_information.continuous_aggregate_policies cap
WHERE cap.view_name IN (
  'symbol_weekly',
  'symbol_monthly',
  'daily_basic_weekly',
  'daily_basic_monthly',
  'adj_factor_weekly',
  'adj_factor_monthly'
)
ORDER BY cap.view_name;
```

**预期结果**:
| 视图 | 刷新滞后 | 结束偏移 | 调度间隔 |
|------|----------|----------|----------|
| symbol_weekly | 1 month | 1 hour | 1 hour |
| symbol_monthly | 3 months | 1 hour | 1 hour |
| daily_basic_weekly | 1 month | 1 hour | 1 hour |
| daily_basic_monthly | 3 months | 1 hour | 1 hour |
| adj_factor_weekly | 1 month | 1 hour | 1 hour |
| adj_factor_monthly | 3 months | 1 hour | 1 hour |

### 步骤 6: 测试数据查询

```bash
# 使用 CLI 工具测试聚合状态
fdh-cli status --aggregates
```

**预期输出**: 显示 4 个聚合视图的状态和大小

---

## 验证功能

### 1. 验证 SDK 查询

```python
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

settings = get_settings()
fdh = FinanceDataHub(settings)

# 测试周线查询
weekly = fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"周线数据: {len(weekly)} 条")

# 测试月线查询
monthly = fdh.get_monthly(['600519.SH'], '2020-01-01', '2024-12-31')
print(f"月线数据: {len(monthly)} 条")
```

### 2. 验证数据准确性

```bash
# 运行验证脚本
python scripts/validate_aggregates.py --symbol 600519.SH --year 2024
```

**预期输出**: 所有验证通过，最大误差 < 0.01%

### 3. 验证手动刷新

```bash
# 手动刷新测试
fdh-cli refresh-aggregates --table symbol_weekly --start 2024-01-01 --end 2024-12-31
```

**预期输出**: 刷新成功完成

---

## 性能影响评估

### 存储开销

```sql
-- 检查聚合存储大小
SELECT
  'symbol_weekly' as aggregate,
  pg_size_pretty(pg_total_relation_size('symbol_weekly')) as size
UNION ALL
SELECT
  'symbol_monthly' as aggregate,
  pg_size_pretty(pg_total_relation_size('symbol_monthly')) as size
UNION ALL
SELECT
  'daily_basic_weekly' as aggregate,
  pg_size_pretty(pg_total_relation_size('daily_basic_weekly')) as size
UNION ALL
SELECT
  'daily_basic_monthly' as aggregate,
  pg_size_pretty(pg_total_relation_size('daily_basic_monthly')) as size
UNION ALL
SELECT
  'adj_factor_weekly' as aggregate,
  pg_size_pretty(pg_total_relation_size('adj_factor_weekly')) as size
UNION ALL
SELECT
  'adj_factor_monthly' as aggregate,
  pg_size_pretty(pg_total_relation_size('adj_factor_monthly')) as size;
```

**预期开销**: 约 5-8 GB（基于 5000 只股票，20 年数据）

### 写入性能影响

连续聚合会增加少量写入开销（通常 < 5%），因为需要：

1. 维护聚合视图的元数据
2. 触发后台刷新作业

**监控写入性能**:
```sql
-- 检查后台作业
SELECT * FROM timescaledb_information.job_status
WHERE job_name = 'Refresh Continuous Aggregate';
```

---

## 回滚程序

如果迁移后遇到问题，可以按以下步骤回滚：

### 方案 1: 删除聚合视图（保留数据）

```sql
-- 删除连续聚合视图
DROP MATERIALIZED VIEW IF EXISTS symbol_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS symbol_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS daily_basic_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS daily_basic_monthly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS adj_factor_weekly CASCADE;
DROP MATERIALIZED VIEW IF EXISTS adj_factor_monthly CASCADE;
```

### 方案 2: 从备份恢复

```bash
# 恢复备份
psql "$DATABASE_URL" < backup_YYYYMMDD_HHMMSS.sql
```

---

## 故障排除

### 问题 1: 聚合创建失败

**错误信息**: `ERROR: cannot create continuous aggregate`

**解决方案**:
1. 确认 TimescaleDB 版本 >= 2.0
2. 检查源表是否为超表:
   ```sql
   SELECT * FROM timescaledb_information.hypertables WHERE hypertable_name IN ('symbol_daily', 'daily_basic');
   ```

### 问题 2: 刷新作业失败

**错误信息**: `WARNING: continuous aggregate refresh job failed`

**解决方案**:
1. 检查源表是否有数据:
   ```sql
   SELECT COUNT(*) FROM symbol_daily;
   ```
2. 手动刷新以获取详细错误:
   ```sql
   CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL);
   ```

### 问题 3: 查询性能差

**解决方案**:
1. 确认索引已创建:
   ```sql
   \d symbol_weekly  -- 应该显示索引
   ```
2. 分析查询计划:
   ```sql
   EXPLAIN ANALYZE SELECT * FROM symbol_weekly WHERE symbol = '600519.SH' LIMIT 10;
   ```

### 问题 4: 数据不准确

**解决方案**:
1. 运行验证脚本检查误差
2. 手动刷新问题范围:
   ```sql
   CALL refresh_continuous_aggregate('symbol_weekly', '2024-01-01', '2024-12-31');
   ```

---

## 监控和维护

### 日常监控

```bash
# 检查聚合状态
fdh-cli status --aggregates

# 检查刷新作业状态
psql "$DATABASE_URL" -c "
SELECT job_id, job_name, status, last_run, next_run
FROM timescaledb_information.job_status
WHERE job_name = 'Refresh Continuous Aggregate';
"
```

### 存储监控

```sql
-- 定期检查聚合存储大小
SELECT
  schemaname,
  tablename,
  pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size
FROM pg_tables
WHERE tablename IN (
  'symbol_weekly',
  'symbol_monthly',
  'daily_basic_weekly',
  'daily_basic_monthly',
  'adj_factor_weekly',
  'adj_factor_monthly'
);
```

### 性能调优

如果聚合刷新影响性能，可以调整刷新策略：

```sql
-- 延长刷新间隔（例如从 1 小时改为 6 小时）
SELECT remove_continuous_aggregate_policy('symbol_weekly', 'policy_refresh_symbol_weekly');
SELECT add_continuous_aggregate_policy('symbol_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '6 hours',
  schedule_interval => INTERVAL '6 hours'
);
```

---

## 下一步

迁移完成后，您可以：

1. **更新应用代码**：使用新的 SDK 方法查询高周期数据
2. **性能测试**：在生产环境中测试查询性能
3. **用户培训**：向用户介绍新的周线、月线数据查询功能
4. **文档更新**：更新相关用户文档

---

## 支持和联系

如果在迁移过程中遇到问题：

1. 查看 [troubleshooting 指南](#故障排除)
2. 运行验证脚本获取详细信息
3. 检查日志文件中的错误信息
4. 联系技术支持团队

---

**迁移完成日期**: ___________
**执行人**: ___________
**验证人**: ___________
