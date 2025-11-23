# frequency 字段方案实施总结

## 问题背景

在原始设计中，`symbol_minute` 表没有 `frequency` 字段，导致所有频率的分钟数据（1m, 5m, 15m, 30m, 60m）混在一起，存在以下问题：

1. **数据冲突风险** - 不同频率的数据可能有相同的 (symbol, time)，导致数据被覆盖
2. **查询困难** - 无法区分不同频率的数据
3. **分区不优化** - TimescaleDB 无法按频率进行二级分区
4. **数据完整性风险** - 容易造成数据混乱

## 解决方案

采用**方案 1：添加 frequency 字段**，在同一张表中存储不同频率的数据，通过三列复合主键 `(symbol, time, frequency)` 确保数据不冲突。

### 优点

- ✅ 代码改动最小
- ✅ 兼容性好（现有数据可平滑迁移）
- ✅ 符合 TimescaleDB 最佳实践（复合分区）
- ✅ 便于跨频率数据分析
- ✅ 不同频率数据不会相互覆盖

### 架构变更

#### 数据库 Schema 变更

**主键变更:**
```sql
-- 修改前
PRIMARY KEY (symbol, time)

-- 修改后
PRIMARY KEY (symbol, time, frequency)
```

**新增字段:**
```sql
frequency VARCHAR(5) NOT NULL  -- 可选值: 1m, 5m, 15m, 30m, 60m
```

**新增索引:**
```sql
CREATE INDEX idx_symbol_minute_freq ON symbol_minute(frequency, symbol, time DESC);
```

**TimescaleDB 分区策略:**
```sql
-- 时间分区（主分区）：1周
-- 频率分区（二级分区）：5个分区
SELECT create_hypertable(
    'symbol_minute',
    'time',
    partitioning_column => 'frequency',
    number_partitions => 5,
    chunk_time_interval => INTERVAL '1 week'
);
```

## 实施清单

### ✅ 已完成的修改

#### 1. 数据库 Schema ([003_create_hypertables.sql](sql/init/003_create_hypertables.sql#L37-L71))

- [x] 添加 `frequency VARCHAR(5) NOT NULL` 字段
- [x] 修改主键为 `(symbol, time, frequency)`
- [x] 添加频率索引 `idx_symbol_minute_freq`
- [x] 配置 TimescaleDB 复合分区

#### 2. 数据库操作层 ([operations.py:162-235](finance_data_hub/database/operations.py#L162-L235))

- [x] `insert_symbol_minute_batch()` 方法签名添加 `freq` 参数
- [x] INSERT 语句添加 `frequency` 字段
- [x] ON CONFLICT 子句更新为 `(symbol, time, frequency)`
- [x] valid_fields 添加 `frequency`
- [x] 自动为记录添加 frequency 值

#### 3. 数据更新层 ([updater.py:275-277](finance_data_hub/update/updater.py#L275-L277))

- [x] 调用 `insert_symbol_minute_batch()` 时传递 `freq` 参数

#### 4. 迁移脚本

已创建以下迁移文件：

- [x] [001_add_frequency_to_symbol_minute.sql](sql/migrations/001_add_frequency_to_symbol_minute.sql) - 迁移脚本
- [x] [001_rollback_frequency.sql](sql/migrations/001_rollback_frequency.sql) - 回滚脚本
- [x] [run_migration.py](sql/migrations/run_migration.py) - Python 迁移工具
- [x] [README.md](sql/migrations/README.md) - 迁移指南

## 使用方法

### 1. 全新部署（推荐）

对于全新的数据库，直接运行初始化脚本即可：

```bash
fdh-cli init
```

新的 schema 会自动创建包含 frequency 字段的表。

### 2. 现有数据库迁移

对于已有数据的数据库，需要执行迁移：

```bash
# 方法 1：使用 Python 脚本（推荐）
python sql/migrations/run_migration.py migrate

# 方法 2：直接使用 psql
psql "$DATABASE_URL" -f sql/migrations/001_add_frequency_to_symbol_minute.sql
```

### 3. 验证迁移

```bash
python sql/migrations/run_migration.py verify
```

### 4. 回滚（如需要）

⚠️ 警告：回滚会删除所有非 1m 频率的数据！

```bash
python sql/migrations/run_migration.py rollback
```

## 数据流

修改后的完整数据流：

```
CLI (--dataset minute_5)
  ↓ 映射为 freq="5m"
  ↓
DataUpdater.update_minute_data(freq="5m")
  ↓
Router.route(data_type="minute", freq="5m")
  ↓ router 将 freq 添加到 kwargs
  ↓
XTQuantProvider.get_minute_data(freq="5m")
  ↓ 映射为 xtquant_freq="5m"
  ↓
xtquant_helper (period="5m")
  ↓ 返回 5分钟数据
  ↓
DataOperations.insert_symbol_minute_batch(data, freq="5m")
  ↓ 自动添加 frequency="5m" 到每条记录
  ↓
PostgreSQL/TimescaleDB
  ↓ 复合主键 (symbol, time, frequency) 确保不冲突
  ↓
按时间+频率分区存储
```

## 测试验证

### 验证不同频率数据共存

```sql
-- 插入测试数据：同一时间点的不同频率数据
INSERT INTO symbol_minute (time, symbol, frequency, open, high, low, close, volume, amount)
VALUES
    ('2025-11-21 09:30:00+08', '600519.SH', '1m', 100.0, 101.0, 99.0, 100.5, 1000, 100000),
    ('2025-11-21 09:30:00+08', '600519.SH', '5m', 100.0, 102.0, 98.0, 101.0, 5000, 500000),
    ('2025-11-21 09:30:00+08', '600519.SH', '15m', 100.0, 103.0, 97.0, 102.0, 15000, 1500000);

-- 验证：应该返回 3 条记录
SELECT time, symbol, frequency, close, volume
FROM symbol_minute
WHERE symbol = '600519.SH' AND time = '2025-11-21 09:30:00+08'
ORDER BY frequency;
```

### 验证查询性能

```sql
-- 测试频率过滤查询
EXPLAIN ANALYZE
SELECT time, symbol, open, close, volume
FROM symbol_minute
WHERE symbol = '600519.SH'
  AND frequency = '5m'
  AND time >= '2025-11-01'
ORDER BY time DESC
LIMIT 100;

-- 应该使用索引 idx_symbol_minute_freq
```

## 性能影响

### 存储开销

- 主键增大：(symbol, time) → (symbol, time, frequency)
- 新增索引：idx_symbol_minute_freq
- 预计存储增加：< 5%

### 写入性能

- 复合主键检查略有开销
- 预计写入性能影响：< 5%

### 查询性能

- ✅ 按频率查询：显著提升（使用频率索引）
- ✅ 按时间+频率查询：提升（复合分区）
- ⚠️ 全表扫描：略有下降（主键变大）

## 注意事项

1. **迁移前务必备份数据**
2. **迁移期间建议停止数据写入**（避免冲突）
3. **迁移后需要执行 ANALYZE**：
   ```sql
   ANALYZE symbol_minute;
   ```
4. **确保查询包含 frequency 条件**（提高性能）
5. **监控迁移日志**（PostgreSQL NOTICE 消息）

## 后续优化建议

1. **查询优化** - 所有分钟数据查询应包含 frequency 条件
2. **监控指标** - 监控不同频率数据的写入和查询性能
3. **数据清理** - 定期清理过期的高频数据（1m, 5m）
4. **压缩策略** - 对历史数据启用 TimescaleDB 压缩

## 相关文件

### 修改的文件

1. [sql/init/003_create_hypertables.sql](sql/init/003_create_hypertables.sql)
2. [finance_data_hub/database/operations.py](finance_data_hub/database/operations.py)
3. [finance_data_hub/update/updater.py](finance_data_hub/update/updater.py)

### 新增的文件

1. [sql/migrations/001_add_frequency_to_symbol_minute.sql](sql/migrations/001_add_frequency_to_symbol_minute.sql)
2. [sql/migrations/001_rollback_frequency.sql](sql/migrations/001_rollback_frequency.sql)
3. [sql/migrations/run_migration.py](sql/migrations/run_migration.py)
4. [sql/migrations/README.md](sql/migrations/README.md)

### 相关的先前修复

1. [frequency_fix_summary.md](frequency_fix_summary.md) - Frequency 参数传递修复
2. [xtquant_fix_summary.md](xtquant_fix_summary.md) - XTQuant 数据转换修复

## 总结

✅ 已完成数据库架构升级，支持多频率分钟数据存储
✅ 提供完整的迁移和回滚方案
✅ 兼容现有代码和数据
✅ 优化了 TimescaleDB 分区策略
✅ 提高了查询性能和数据完整性

现在可以安全地存储和查询不同频率的分钟数据，不会发生数据覆盖！
