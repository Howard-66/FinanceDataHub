# 基金业绩基准与持仓

FinanceDataHub 支持同步和查询 Tushare `mkt_idx_bmk` ETF 业绩比较基准库及 `fund_portfolio` 公募基金季度持仓。

## ETF 业绩比较基准（`mkt_idx_bmk`）

接口返回指数代码、简称和全称、基准库层级与类型、编制机构、指数类型等全部 8 个输出字段。Tushare 单次最多返回 500 条，当前完整基准库可一次获取；调用需要 5000 积分。

```bash
# 同步全部基准库
fdh-cli update --dataset mkt_idx_bmk

# 同步一个指数
fdh-cli update --dataset mkt_idx_bmk --symbols 000300.SH
```

```python
benchmarks = fdh.get_mkt_idx_bmk(bmk_level="一类库", bmk_type="宽基")
benchmarks_async = await fdh.get_mkt_idx_bmk_async(ts_code="000300.SH")
```

数据写入 `mkt_idx_bmk` 表，以 `ts_code` 为主键 upsert。调度任务 `mkt_idx_bmk_update` 默认在每周一 07:20 执行。

## 公募基金持仓（`fund_portfolio`）

该接口按季度更新，保留全部 8 个输出字段：`ts_code`、`ann_date`、`end_date`、`symbol`、`mkv`、`amount`、`stk_mkv_ratio`、`stk_float_ratio`。接口至少要提供基金代码、公告日期或报告期之一；5000 积分账户限 200 次/分钟，8000 积分账户限 500 次/分钟。

```bash
# 同步一个或多个基金的持仓
fdh-cli update --dataset fund_portfolio --symbols 001753.OF,000001.OF

# 同步指定报告期全部披露的持仓
fdh-cli update --dataset fund_portfolio --trade-date 2024-06-30
```

```python
holdings = fdh.get_fund_portfolio(
    ts_code="001753.OF", start_date="2024-01-01", end_date="2024-12-31"
)
holdings_async = await fdh.get_fund_portfolio_async(period="20240630")
```

数据写入 `fund_portfolio`，以 `(ts_code, ann_date, end_date, symbol)` 为联合主键 upsert。`fund_portfolio_update` 默认禁用，因为运行时必须明确报告期或基金范围；启用前请为调度器提供相应参数。

已有部署请执行迁移 [034_create_fund_benchmark_portfolio.sql](../../sql/migrations/034_create_fund_benchmark_portfolio.sql)。
