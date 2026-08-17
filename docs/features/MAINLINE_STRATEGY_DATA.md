# 量化主线策略数据支持

FinanceDataHub 为“个人投资者量化主线策略”提供事实数据、点时过滤字段和预计算因子；行业评分、组合构建和交易规则仍由策略项目负责。

## 已实现范围

### 原始补充数据

| 数据集 | 表 | 用途 | 历史边界 |
|---|---|---|---|
| 股票上市状态 | `asset_basic` | 保留 L/D/P，消除退市股票幸存者偏差 | 由股票基础接口决定 |
| ST 快照 | `stock_st` | 每日 ST 排除 | 官方数据自 2016-01-01；更早由历史简称重建 |
| 历史简称 | `stock_namechange` | 2012—2015 ST 重建、点时名称 | 接口可用全历史 |
| 停复牌 | `stock_suspend` | 不可交易过滤 | 接口可用历史 |
| 分红 | `stock_dividend` | 股东回报事件 | 接口可用历史 |
| 回购 | `stock_repurchase` | 股东回报事件 | 接口可用历史 |
| 融资融券 | `margin_detail` | 个股/行业资金共识 | 接口可用历史 |
| 沪深港通 | `moneyflow_hsgt` | 市场级北向资金 | 仅市场汇总，不等同于当前个股北向持仓 |
| 申万成分 | `sw_industry_member` | SW2021 L2 点时行业归属 | 同一股票可保存多段纳入/剔除区间 |

申万查询支持 `as_of`：区间条件为 `in_date <= as_of <= out_date`，`out_date` 为空表示仍有效。

早期分红记录偶尔缺少预案公告日。为保留历史事件且防止未来数据泄漏，
`stock_dividend.ann_date` 在此情况下使用实施公告日 `imp_ann_date`；两者都缺失的记录不进入策略原始表。

### 预处理数据

| SDK dataset | 表 | 内容 |
|---|---|---|
| `stock_daily` | `processed_mainline_stock_daily` | 点时可交易性、SW2021 L1/L2、动量、波动、回撤、估值、财务质量、融资、分红/回购事件 |
| `market_daily` | `processed_mainline_market_daily` | 中证全指基准、20/60/120 日趋势、20/60 日广度、涨跌比、北向资金、市场状态原始标签 |
| `industry_daily` | `processed_mainline_industry_daily` | SW2021 L2 收益、相对动量、广度、成交额占比、估值/质量中位数、融资变化 |
| `etf_daily` | `processed_mainline_etf_daily` | 复权动量、流动性、规模、份额变化、跟踪误差和明确排除原因 |
| `fund_crowding_monthly` | `processed_mainline_fund_crowding_monthly` | 按实际公告可用日控制的基金持仓拥挤度 |
| `leadlag_monthly` | `processed_mainline_leadlag_monthly` | 供确定候选产业链后写入的滚动领先滞后结果 |
| `data_status` | `processed_mainline_data_status` | 覆盖率、新鲜度、合格/排除数量和审计信息 |

ETF 不使用代理基准。`index_code` 缺失或基准日线不存在时，`is_eligible=false`，并分别记录 `missing_index_code` 或 `missing_benchmark_daily`。

## 数据库迁移（必须人工执行）

现有数据库需由数据库管理员执行：

```bash
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f sql/migrations/037_create_mainline_strategy_data.sql
```

迁移会修改 `sw_industry_member` 的主键结构并创建新表，因此必须先做数据库备份。迁移是幂等的，但不应与行业成分更新任务并发执行。

新建 Docker 数据库会通过 `docker-compose.yml` 自动挂载同一迁移脚本；这不影响已有数据库。

## 首次回填顺序

迁移完成后，在项目根目录依次执行：

```bash
# 1. 完整股票目录（包含上市、退市、暂停）
fdh-cli update --dataset basic --asset-class stock --market CN --force

# 2. SW2021 分类与完整历史成分区间
fdh-cli update --dataset sw_industry_classify --asset-class index --force
fdh-cli update --dataset sw_industry_member --asset-class index --force

# 3. 状态与事件。ST 官方历史从 2016 开始；其余策略回测从 2012 开始。
fdh-cli update --dataset stock_st --asset-class stock --start-date 2016-01-01 --force
fdh-cli update --dataset stock_namechange --asset-class stock --force
fdh-cli update --dataset stock_suspend --asset-class stock --start-date 2012-01-01 --force
fdh-cli update --dataset stock_dividend --asset-class stock --force
fdh-cli update --dataset stock_repurchase --asset-class stock --start-date 2012-01-01 --force
fdh-cli update --dataset margin_detail --asset-class stock --start-date 2012-01-01 --force
fdh-cli update --dataset moneyflow_hsgt --asset-class stock --start-date 2012-01-01 --force

# 4. 基础技术/估值/基本面预处理完成后，生成主线因子。
fdh-cli preprocess run --category mainline --all \
  --start-date 2012-01-01 --end-date "$(date +%F)"
```

主线预处理按自然月分区，每个分区依次提交股票、市场、行业、ETF 和基金拥挤度结果，
命令行显示当前数据集、日期区间及 `x/y 分区`。中断后已完成分区会保留，重跑时通过 UPSERT 幂等更新。
`--force` 且未指定 `--start-date` 时，主线预处理固定从 `2012-01-01` 开始；非强制模式未指定日期时，默认只重算最近 400 天。
`fund_crowding_monthly` 只在区间覆盖基金持仓报告期时产生记录；`leadlag_monthly` 需由策略侧提供候选关系，不在该全量命令中自动生成。

大范围接口回填耗时较长，建议先用一个月区间验证权限与表结构，再执行全历史。

## SDK 示例

```python
# 点时申万行业归属
members = await fdh.get_sw_industry_members_async(
    ts_code="600519.SH", as_of="2018-06-29"
)

# 原始事件
repurchase = await fdh.get_mainline_raw_async(
    "stock_repurchase", symbols=["600519.SH"],
    start_date="2018-01-01", end_date="2025-12-31"
)

# 只取当日可交易的股票因子
stocks = await fdh.get_processed_mainline_async(
    "stock_daily", start_date="2025-01-01", end_date="2025-12-31",
    eligible_only=True,
)

# ETF 表包含不合格记录及排除原因，便于审计
etfs = await fdh.get_processed_mainline_async(
    "etf_daily", start_date="2025-01-01", end_date="2025-12-31"
)
```

## 明确缺口与降级规则

- 无分析师一致预期数据：不伪造，第一版使用已公告财务数据和价格/资金因子。
- 无结构化产业链上下游关系：`calculate_leadlag_monthly` 只计算调用方明确给出的候选关系；不对全市场盲目做笛卡尔积。
- 无当前个股级北向持仓：仅保留市场级 `moneyflow_hsgt`，个股/行业资金项以融资余额、ETF 份额和成交额替代。
- 2016 年前无官方 ST 快照：使用 `namechange` 中包含 ST/*ST 的有效简称区间重建，并通过 `st_source=namechange_reconstructed` 标记。
- ETF 缺跟踪基准：直接排除并记录原因，不使用 Wind 或主观代理指数。

## 运行边界

- 生产因子，不生产行业总分、股票总分、仓位和买卖信号。
- 所有公司事件只在公告日及之后可见；基金拥挤度只在完整披露的最晚公告日及之后可用。
- 历史回测起点为 2012-01-01；预处理会自动拉取额外窗口用于 120 日指标预热。
