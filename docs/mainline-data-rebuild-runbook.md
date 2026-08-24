# FinanceDataHub 主线策略数据层重建运行清单

`038_rebuild_mainline_pit_factor_tables.sql` 已重建的对象仅为
`processed_mainline_*` 派生表；原始事实表不会被删除。全量回填期间不要执行
`039_add_mainline_factor_query_indexes.sql`，避免索引拖慢写入。

资金流增强迁移在 038 之后执行；它只增加标准 Tushare `moneyflow`，不会接入
THS/DC、概念板块或大盘资金流。

```bash
export MAINLINE_END_DATE=2026-08-20 # 改为执行时最近已完成交易日
# 通过 python-dotenv 读取，避免 `.env` 中非 POSIX 格式的第三方 API 配置影响 shell。
DATABASE_URL="$(uv run python -c 'from dotenv import dotenv_values; print(dotenv_values(".env")["DATABASE_URL"].replace("postgresql+asyncpg://", "postgresql://"))')"
PSQL=/opt/homebrew/opt/libpq/bin/psql
```

本地 `sources.yml`（该文件刻意不纳入 Git）需包含以下路由；新环境可从
`sources.yml.example` 复制：

```yaml
routing_strategy:
  stock:
    CN:
      moneyflow:
        providers: [tushare]
        fallback: false
```

## 1. 结构迁移

```bash
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f /Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sql/migrations/038_rebuild_mainline_pit_factor_tables.sql

"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f /Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sql/migrations/040_add_mainline_standard_moneyflow.sql
```

## 2. 全量补齐原始事实和基础底座

```bash
fdh-cli update --dataset trade_cal --symbols SSE,SZSE --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset basic --asset-class stock --market CN --force
fdh-cli update --dataset daily --market CN --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset adj_factor --market CN --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset daily_basic --market CN --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset income --force
fdh-cli update --dataset balancesheet --force
fdh-cli update --dataset cashflow --force
fdh-cli update --dataset fina_indicator --force

fdh-cli update --dataset index_basic --symbols CSI,SW
fdh-cli update --dataset index_daily --symbols 000985.CSI --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset sw_daily --symbols all --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset index_weight --symbols all --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset etf_basic --symbols all
fdh-cli update --dataset etf_index --symbols all
fdh-cli update --dataset mkt_idx_bmk --symbols all
fdh-cli update --dataset fund_daily --symbols all --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset fund_adj --symbols all --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset etf_share_size --symbols all --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset fund_nav --symbols all --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset etf_sh_cons --symbols all --force
fdh-cli update --dataset etf_sz_cons --symbols all --force
fdh-cli update --dataset idx_anns --symbols all --force

fdh-cli update --dataset stock_st --asset-class stock --start-date 2016-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset stock_namechange --asset-class stock --force
fdh-cli update --dataset stock_suspend --asset-class stock --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset stock_dividend --asset-class stock --force
fdh-cli update --dataset stock_repurchase --asset-class stock --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset margin_detail --asset-class stock --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset moneyflow_hsgt --asset-class stock --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
# 标准个股资金流按股票代码拉取；不传 --start-date 时，沪市从 2007 年、
# 深市从 2010 年开始，并自动取股票上市日中的较晚者。
fdh-cli update --dataset moneyflow --asset-class stock --symbols all --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset fund_portfolio --symbols all --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
```

## 3. 预处理与 PIT 快照发布

```bash
fdh-cli preprocess run --category technical --all --start-date 2010-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category valuation_fill --all --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category fundamental --all --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category quarterly_fundamental --all --force
fdh-cli preprocess run --category industry_valuation --all --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force

fdh-cli preprocess run --category mainline --stage daily,crowding --all --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category mainline --stage leadlag --all --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category mainline --stage publish --all --end-date "$MAINLINE_END_DATE"
fdh-cli preprocess status
```

## 4. 按需历史修复（不进入定时调度）

历史修复只在原始数据被追溯更正、因子公式/表结构变更，或覆盖率审计发现缺口时
执行；日常调度不会再重复回放 2012 年以来的全部数据。先选择最早受影响日期，并
额外向前留出 420 个自然日的滚动窗口；只有首次建库或覆盖全历史的修复才从 2012 年
开始。以下示例保留了完整的状态、公司行为和资金流修复链，未受影响的数据集可跳过：

```bash
export MAINLINE_END_DATE="$(date +%F)"
export REPAIR_START=2012-01-01 # 局部修复改为“最早受影响日 - 420 天”

fdh-cli update --dataset basic --asset-class stock --market CN --force
fdh-cli update --dataset sw_industry_classify --asset-class index --force
fdh-cli update --dataset sw_industry_member --asset-class index --force
fdh-cli update --dataset stock_st --asset-class stock --start-date 2016-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset stock_namechange --asset-class stock --force
fdh-cli update --dataset stock_suspend --asset-class stock --start-date "$REPAIR_START" --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset stock_dividend --asset-class stock --force
fdh-cli update --dataset stock_repurchase --asset-class stock --start-date "$REPAIR_START" --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset margin_detail --asset-class stock --start-date "$REPAIR_START" --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset moneyflow_hsgt --asset-class stock --start-date "$REPAIR_START" --end-date "$MAINLINE_END_DATE" --force
fdh-cli update --dataset moneyflow --asset-class stock --symbols all --end-date "$MAINLINE_END_DATE" --force

fdh-cli preprocess run --category mainline --stage daily,crowding --all \
  --start-date "$REPAIR_START" --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category mainline --stage leadlag --all \
  --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess run --category mainline --stage publish --all \
  --end-date "$MAINLINE_END_DATE"
```

`crowding` 日常任务以 `fund_portfolio.updated_at` 的近 10 个交易日为输入，只重算
受影响的报告期；上面的人工修复不传该参数，因此按 `REPAIR_START` 至结束日完整重算。

## 5. 分区与局部恢复

全量跨度超过两年时，主线预处理会自动采用六个月分区（短期增量仍按月），
以避免每月重复扫描 120～420 日滚动窗口；终端进度的分区总数会相应下降。
修改代码前已经启动的任务不会自动切换该策略，应停止后从同一命令重新执行。

若全量任务仅在 `etf_daily` 失败，可只回补失败的时间分区，避免重跑股票、
市场和行业层；`etf` 同时重建 ETF 日表及其行业暴露：

```bash
fdh-cli preprocess run --category mainline --stage etf,crowding --all \
  --start-date 2026-07-01 --end-date "$MAINLINE_END_DATE" --force
```

若行业日表为空、且 `leadlag` 返回零记录，先确认 `stock_daily` 的申万 L2
归属已正确写入；行业因子以个股和市场层为输入。若需要从 2012 年回测首月
就有领先滞后信号，必须从 2008 年开始重建这三层作为 756 交易日训练预热。
以下命令不重跑 ETF 日表：

```bash
fdh-cli preprocess run --category mainline --stage stock,market,industry --all \
  --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force

fdh-cli preprocess run --category mainline --stage leadlag --all \
  --start-date 2008-01-01 --end-date "$MAINLINE_END_DATE" --force
```

若只接受 2012 年之后积累训练样本，可把上述两个命令的开始日期改为
`2012-01-01`；模型最少需要 252 个有效交易日，早期月份不会产生信号。

## 6. 验收后创建查询索引

```bash
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f /Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sql/migrations/039_add_mainline_factor_query_indexes.sql
```

验收查询的核心条件：`industry_daily` 每个已完成交易日恰好 124 个
`is_pub='1'` 的 SW2021 L2 行业；策略与回测调用 SDK 时传入
`usable_on_or_before=<执行日>`；只读取 `v_mainline_ready_snapshot` 对应的
`ready` 版本。
