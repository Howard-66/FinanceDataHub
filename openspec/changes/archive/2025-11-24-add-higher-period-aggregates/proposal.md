# 提案：添加高周期数据聚合

## 摘要
使用 TimescaleDB Continuous Aggregates（连续聚合）实现高周期数据合成（周线、月线），提供预计算的聚合市场数据，用于分析和回测。

## 问题陈述
当前系统将日线和分钟级市场数据存储在 `symbol_daily` 和 `symbol_minute` 超表中。当用户需要周线或月线数据用于：
- 长期趋势分析
- 组合再平衡信号
- 策略回测

他们必须：
1. 下载大量日线数据并在应用层进行重采样（缓慢，资源密集）
2. 编写复杂的 SQL 聚合查询（重复查询效率低下）

这造成了性能瓶颈和用户体验不佳。

## 提议解决方案
利用 **TimescaleDB Continuous Aggregates**（连续聚合）创建自动维护的物化视图：
- 周线 OHLCV 数据（`symbol_weekly`）
- 月线 OHLCV 数据（`symbol_monthly`）
- 聚合的每日基础指标（`daily_basic_weekly`、`daily_basic_monthly`）

### 为什么选择连续聚合？

| 方案 | 优点 | 缺点 |
|------|------|------|
| 应用层重采样 | 灵活，无需数据库修改 | 大数据量时性能差，客户端资源消耗大 |
| 普通视图 | 简单，无需存储 | 每次查询都计算，性能差 |
| **连续聚合** | **预计算，自动刷新，TimescaleDB 原生** | **额外存储，初始设置** |
| 独立 ETL 表 | 完全控制 | 手动一致性管理，复杂 |

**连续聚合是最优选择**，因为：
1. 项目已使用 TimescaleDB 超表
2. 数据插入时自动增量更新
3. 查询性能媲美常规表
4. 原生支持时间序列聚合函数

## 范围

### 范围内
- 创建周线/月线价格数据的连续聚合
- 创建周线/月线每日基础指标的连续聚合
- 配置自动刷新策略
- 添加查询高周期数据的 SDK 方法
- 记录聚合逻辑和使用方法

### 范围外
- 季度/年线聚合（后续可添加）
- 用户自定义周期
- 实时流式聚合
- Parquet/DuckDB 冷存储同步（第四阶段关注点）

## 技术方案

### 聚合逻辑
```sql
-- 周线聚合示例（含复权处理）
CREATE MATERIALIZED VIEW symbol_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  -- 复权价格：使用期间最后一天的复权因子
  first(open, time) * last(adj_factor, time) / first(adj_factor, time) AS open,
  max(high) * last(adj_factor, time) / first(adj_factor, time) AS high,
  min(low) * last(adj_factor, time) / first(adj_factor, time) AS low,
  last(close, time) AS close,
  sum(volume) AS volume,
  sum(amount) AS amount,
  last(adj_factor, time) AS adj_factor
FROM symbol_daily
GROUP BY time_bucket('1 week', time), symbol;
```

### 刷新策略
- **实时物化**：启用
- **刷新滞后**：1 小时（处理延迟数据修正）
- **刷新间隔**：1 小时

### SDK 集成
```python
# 新的 SDK 方法
fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
fdh.get_monthly(['600519.SH'], '2024-01-01', '2024-12-31')
fdh.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
```

## 影响分析

### 数据库
- 新增连续聚合（共计 4 个）
- 额外存储：约为源数据大小的 5-10%
- 对现有表无影响

### 性能
- 查询改进：周线/月线数据查询提升 10-100 倍
- 最小写入开销（自动后台刷新）

### API
- 新增高周期数据查询的 SDK 方法
- 向后兼容，无破坏性变更

## 考虑的替代方案

### 1. 应用层 Pandas 重采样
- 拒绝：大数据集性能差，浪费带宽

### 2. 普通 PostgreSQL 视图
- 拒绝：每次查询重新计算，无性能提升

### 3. 带 ETL 作业的独立表
- 拒绝：手动一致性管理，复杂度增加

## 依赖关系
- TimescaleDB 扩展（已安装）
- 现有的 `symbol_daily` 和 `daily_basic` 超表

## 风险和缓解措施

| 风险 | 缓解措施 |
|------|----------|
| 存储增长 | 使用 `hypertable_size()` 监控，必要时实施保留策略 |
| 刷新滞后 | 根据用例配置适当的刷新间隔 |
| 复杂聚合查询 | 清晰记录，提供 SDK 抽象层 |

## 成功标准
1. 单只股票 5 年范围的周线/月线数据查询在 100ms 内返回
2. 连续聚合在源数据变更后 1 小时内自动刷新
3. SDK 方法提供直观的聚合数据访问
4. 复权后价格数据准确性匹配手动 Pandas 重采样结果（容差 < 0.01%）
