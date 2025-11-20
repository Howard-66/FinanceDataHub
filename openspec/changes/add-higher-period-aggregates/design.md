# 设计：高周期数据聚合

## 架构概览

```
┌─────────────────────┐
│   symbol_daily      │  (源超表)
│   daily_basic       │
└──────────┬──────────┘
           │ TimescaleDB
           │ 连续聚合
           ▼
┌─────────────────────────────────────┐
│  symbol_weekly    symbol_monthly    │
│  daily_basic_weekly daily_basic_monthly│
└──────────┬──────────────────────────┘
           │
           ▼
┌─────────────────────┐
│   FinanceDataHub    │
│   Python SDK        │
└─────────────────────┘
```

## 连续聚合定义

### 1. symbol_weekly

```sql
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
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

-- 刷新策略：每 1 小时刷新，带 1 小时滞后
SELECT add_continuous_aggregate_policy('symbol_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

-- 创建索引以优化查询
CREATE INDEX ON symbol_weekly (symbol, time DESC);
```

### 2. symbol_monthly

```sql
CREATE MATERIALIZED VIEW symbol_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
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
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('symbol_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

CREATE INDEX ON symbol_monthly (symbol, time DESC);
```

### 3. daily_basic_weekly

```sql
CREATE MATERIALIZED VIEW daily_basic_weekly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 week', time) AS time,
  symbol,
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 week', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_basic_weekly',
  start_offset => INTERVAL '1 month',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

CREATE INDEX ON daily_basic_weekly (symbol, time DESC);
```

### 4. daily_basic_monthly

```sql
CREATE MATERIALIZED VIEW daily_basic_monthly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket('1 month', time) AS time,
  symbol,
  avg(turnover_rate) AS avg_turnover_rate,
  avg(volume_ratio) AS avg_volume_ratio,
  avg(pe) AS avg_pe,
  avg(pe_ttm) AS avg_pe_ttm,
  avg(pb) AS avg_pb,
  avg(ps) AS avg_ps,
  avg(ps_ttm) AS avg_ps_ttm,
  avg(dv_ratio) AS avg_dv_ratio,
  avg(dv_ttm) AS avg_dv_ttm,
  last(total_share, time) AS total_share,
  last(float_share, time) AS float_share,
  last(free_share, time) AS free_share,
  last(total_mv, time) AS total_mv,
  last(circ_mv, time) AS circ_mv
FROM daily_basic
GROUP BY time_bucket('1 month', time), symbol
WITH NO DATA;

SELECT add_continuous_aggregate_policy('daily_basic_monthly',
  start_offset => INTERVAL '3 months',
  end_offset => INTERVAL '1 hour',
  schedule_interval => INTERVAL '1 hour'
);

CREATE INDEX ON daily_basic_monthly (symbol, time DESC);
```

## 聚合逻辑说明

### OHLCV 聚合（含复权处理）
- **开盘价 (Open)**：期间第一天的价格，**应用期间最后一天的复权因子**
- **最高价 (High)**：期间的最高价，**应用期间最后一天的复权因子**
- **最低价 (Low)**：期间的最低价，**应用期间最后一天的复权因子**
- **收盘价 (Close)**：期间最后一天的收盘价，**应用期间最后一天的复权因子**
- **成交量/成交额 (Volume/Amount)**：所有值求和（无复权）
- **复权因子 (Adj Factor)**：期间最后值（表示期末因子）

### 复权计算公式
```sql
-- 正确的聚合应使用：
first(open, time) * last(adj_factor, time) / first(adj_factor, time) AS open,
max(high) * last(adj_factor, time) / first(adj_factor, time) AS high,
min(low) * last(adj_factor, time) / first(adj_factor, time) AS low,
last(close, time) * last(adj_factor, time) / last(adj_factor, time) AS close  -- 即 last(close, time)
```

### 每日基础指标聚合
- **换手率/量比/PE/PB/PS**：期间平均值 (`avg()`)
- **股本/市值**：期间最后值（时点快照）

## 周边界考虑

TimescaleDB `time_bucket('1 week', time)` 默认以周一为周开始（ISO 8601）。

为了与中国市场对齐（交易周一至周五）：
- 周一开始的周与交易周对齐良好
- 无需特殊配置

如果将来需要周日开始：
```sql
time_bucket('1 week', time, TIMESTAMP '2000-01-02')  -- 偏移到周日
```

## 刷新策略

### 策略配置
| 聚合 | start_offset | end_offset | schedule_interval |
|-----------|-------------|------------|-------------------|
| 周线 | 1 month | 1 hour | 1 hour |
| 月线 | 3 months | 1 hour | 1 hour |

### 原理
- **start_offset**：每次刷新回溯多远（覆盖延迟数据修正）
- **end_offset**：物化前的滞后（允许数据结算）
- **schedule_interval**：刷新作业运行频率

### 手动刷新
批量数据加载后的立即刷新：
```sql
CALL refresh_continuous_aggregate('symbol_weekly', '2024-01-01', '2024-12-31');
```

## SDK 实现

### 新增 DataOperations 方法

```python
async def get_weekly_data(
    self,
    symbols: List[str],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取周线聚合 OHLCV 数据."""
    query = text("""
        SELECT time, symbol, open, high, low, close, volume, amount, adj_factor
        FROM symbol_weekly
        WHERE symbol = ANY(:symbols)
        AND time BETWEEN :start_date AND :end_date
        ORDER BY symbol, time
    """)
    # ... 执行并返回 DataFrame

async def get_monthly_data(
    self,
    symbols: List[str],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取月线聚合 OHLCV 数据."""
    # 类似实现

async def get_daily_basic_weekly(
    self,
    symbols: List[str],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取周线聚合每日基础指标."""
    # 类似实现

async def get_daily_basic_monthly(
    self,
    symbols: List[str],
    start_date: str,
    end_date: str
) -> pd.DataFrame:
    """获取月线聚合每日基础指标."""
    # 类似实现
```

### FinanceDataHub SDK 方法

```python
def get_weekly(self, symbols, start_date, end_date):
    """周线数据同步包装器."""
    return asyncio.run(self.ops.get_weekly_data(symbols, start_date, end_date))

async def get_weekly_async(self, symbols, start_date, end_date):
    """周线数据异步方法."""
    return await self.ops.get_weekly_data(symbols, start_date, end_date)

# 月线、每日基础周线、每日基础月线类似
```

## 存储估算

基于典型 A 股市场数据：
- ~5000 支股票
- ~20 年日线数据

| 表 | 行数 | 估算大小 |
|-------|------|----------------|
| symbol_daily | ~2500万 | ~5 GB |
| symbol_weekly | ~500万 | ~1 GB |
| symbol_monthly | ~120万 | ~250 MB |
| daily_basic | ~2500万 | ~6 GB |
| daily_basic_weekly | ~500万 | ~1.2 GB |
| daily_basic_monthly | ~120万 | ~300 MB |

**总额外存储**：~3 GB（< 10% 开销）

## 迁移计划

### 现有数据库
1. 运行 SQL 脚本创建连续聚合（幂等）
2. 执行初始数据填充：
   ```sql
   CALL refresh_continuous_aggregate('symbol_weekly', NULL, NULL);
   ```
3. 启用刷新策略

### 数据库初始化
添加新 SQL 文件：`006_create_continuous_aggregates.sql`
- 在超表存在后加载
- 使用 `IF NOT EXISTS` 保证幂等性

## 监控

### 检查聚合状态
```sql
SELECT * FROM timescaledb_information.continuous_aggregates;
```

### 检查刷新作业
```sql
SELECT * FROM timescaledb_information.jobs
WHERE proc_name = 'policy_refresh_continuous_aggregate';
```

### 检查聚合大小
```sql
SELECT hypertable_size('symbol_weekly');
```

## 未来考虑

1. **季度/年线聚合**：添加类似的连续聚合
2. **自定义周期**：使用带 time_bucket 参数的 SQL 函数
3. **实时聚合**：启用 `timescaledb.materialized_only = false`
4. **压缩**：在保留期后应用 TimescaleDB 压缩
