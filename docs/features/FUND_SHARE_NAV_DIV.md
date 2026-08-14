# 公募基金规模、净值与分红

FinanceDataHub 支持 Tushare 公募基金的 `fund_share`、`fund_nav` 和 `fund_div` 接口，分别保存接口的全部输出字段。

## 同步

```bash
# 基金规模：支持基金代码、交易日或日期区间；单页上限为 2,000 条，自动 offset 分页
fdh-cli update --dataset fund_share --symbols 150018.SZ --start-date 2024-01-01 --end-date 2024-12-31
# 基金规模全量：按交易日拉取全部基金
fdh-cli update --dataset fund_share --symbols all --force

# 基金净值：基金代码或净值日期（场内 E、场外 O 可通过 --market 指定）
fdh-cli update --dataset fund_nav --symbols 165509.SZ --start-date 2024-01-01 --end-date 2024-12-31

# 基金净值全量：按净值日而不是按基金代码下载（先同步 fund_basic）
fdh-cli update --dataset fund_basic --symbols all
fdh-cli update --dataset fund_nav --symbols all --force
# 可指定范围，缩小首次回补时间
fdh-cli update --dataset fund_nav --symbols all --start-date 2024-01-01 --end-date 2024-12-31 --force

# 基金分红：基金代码或公告日（--trade-date 映射为 ann_date）
fdh-cli update --dataset fund_div --symbols 161618.OF
fdh-cli update --dataset fund_div --trade-date 2024-01-02
# 基金分红全量：按公告日拉取全部基金
fdh-cli update --dataset fund_div --symbols all --force
```

`--symbols all` 的起点均取本地 `fund_basic` 最早的 `found_date`（缺失时回退到 `list_date`、`issue_date`）。`fund_share` 和 `fund_nav` 以 SSE 交易日从历史到最新遍历（交易日历未完整覆盖时回退为工作日）；`fund_div` 按每个自然日遍历，确保不会遗漏非交易日公告。该策略以日期数而非“基金数 × 日期数”发起请求。

单日满页时，`fund_share` 按 2,000 条、`fund_nav` 按 10,500 条、`fund_div` 按 1,000 条使用 `offset` 继续获取后续页面。全量模式会显示已下载日期计数。

调度任务默认启用：`fund_nav_update` 每日 19:00、`fund_share_update` 每日 19:10，均按最近交易日下载全市场数据；`fund_div_update` 每日 19:20 按运行当日的自然日下载公告，避免遗漏周末或节假日公告（Asia/Shanghai）。

## SDK

```python
shares = fdh.get_fund_share(ts_code="150018.SZ")
nav = fdh.get_fund_nav(ts_code="165509.SZ")
dividends = fdh.get_fund_div(ts_code="161618.OF")

nav_async = await fdh.get_fund_nav_async(ts_code="165509.SZ")
```

本地表及主键为：`fund_share(ts_code, trade_date)`、`fund_nav(ts_code, nav_date)` 和 `fund_div(ts_code, ann_date)`。已有数据库执行迁移 [033_create_fund_share_nav_div.sql](../../sql/migrations/033_create_fund_share_nav_div.sql)。
