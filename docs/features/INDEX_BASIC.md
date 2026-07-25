# 指数基本信息（`index_basic`）

FinanceDataHub 现已将 Tushare `index_basic` 作为独立的静态元数据数据集同步到 `index_basic` 表。接口定义以 [Tushare 指数基本信息文档](https://tushare.pro/document/2?doc_id=94) 为准。

## 更新

默认刷新全部市场：

```bash
fdh-cli update --dataset index_basic
```

可通过 `--symbols` 指定一个或多个 Tushare 指数市场代码：

```bash
fdh-cli update --dataset index_basic --symbols SSE,SW
```

支持 `MSCI`、`CSI`、`SSE`、`SZSE`、`CICC`、`SW` 和 `OTH`。该数据集不是时间序列，因而不支持 `--trade-date`、`--start-date` 或 `--end-date`；每次更新都会对选定市场做全量 upsert。

## SDK 查询

```python
from finance_data_hub import FinanceDataHub

# 所有已同步指数
all_indexes = fdh.get_index_basic()

# 按 Tushare 市场、发布商、类别或单个 TS 代码筛选
sw_indexes = fdh.get_index_basic(market="SW")
hs300 = fdh.get_index_basic(ts_code="000300.SH")

# 异步接口
indexes = await fdh.get_index_basic_async(market="CSI", category="规模指数")
```

返回字段为：`ts_code`、`name`、`fullname`、`market`、`publisher`、`index_type`、`category`、`base_date`、`base_point`、`list_date`、`weight_rule`、`desc` 和 `exp_date`。

## 配置和调度

`sources.yml.example` 已为 `index.basic` 配置 Tushare 路由。默认 `schedules.yml` 中的 `index_basic_update` 任务在每个交易日 21:20（Asia/Shanghai）刷新完整指数目录，随后 21:30 执行 `index_daily_update`。

已有数据库请先应用 [`030_create_index_basic.sql`](../../sql/migrations/030_create_index_basic.sql)；新数据库会通过初始化脚本自动创建该表。
