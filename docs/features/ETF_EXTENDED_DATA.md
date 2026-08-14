# ETF 扩展数据支持

FinanceDataHub 支持以下 Tushare ETF/指数接口，并保存官方定义的全部输出字段。

| 数据集 | 单次上限 | 全量策略 | 日常智能增量 |
|---|---:|---|---|
| `etf_index` | 5,000 | 无参数全表快照 + offset | 全表 upsert（接口无可靠修改时间） |
| `fund_daily` | 5,000 | 遍历本地 `etf_basic` 全部代码 | 从本地最新交易日开始逐 SSE 交易日 |
| `fund_adj` | 2,000 | 从最早 ETF 日期开始逐 SSE 交易日 | 从本地最新交易日开始，含最新日重拉 |
| `etf_share_size` | 5,000 | 遍历本地 `etf_basic` 全部代码 | 逐交易日，并回看 7 个自然日以覆盖迟到数据 |
| `etf_sh_cons` | 3,000 | 遍历沪市 ETF 代码，满页时递归拆分日期范围 | 从本地最新交易日起按沪市 ETF 代码更新 |
| `etf_sz_cons` | 3,000 | 遍历深市 ETF 代码，满页时递归拆分日期范围 | 从本地最新交易日起按深市 ETF 代码更新 |
| `idx_anns` | 1,000 | 从 SSE 交易日历最早日期开始，按自然月窗口 | 从最新公告日回看 7 天，按自然月窗口 |

所有 Provider 请求都会显式传入完整字段列表。支持 `offset` 的接口在达到上限时自动
翻页；沪深 ETF 持仓组合接口没有 `offset` 输入，改为按 ETF 代码查询，并在日期区间
达到 3,000 条时递归拆分区间。每个 ETF 批次会立即 upsert，而不是等全量任务结束后
统一写库。

## CLI

```bash
# 先准备 ETF 目录（代码遍历型全量任务依赖它）
fdh-cli update --dataset etf_basic --symbols all

# 全量回补；start/end 省略时自动覆盖完整范围
fdh-cli update --dataset fund_daily --symbols all --force
fdh-cli update --dataset fund_adj --symbols all --force
fdh-cli update --dataset etf_share_size --symbols all --force
fdh-cli update --dataset etf_sh_cons --symbols all --force
fdh-cli update --dataset etf_sz_cons --symbols all --force
fdh-cli update --dataset idx_anns --symbols all --force

# 无代码、无 force：按数据库检查点智能增量
fdh-cli update --dataset fund_daily
fdh-cli update --dataset fund_adj
fdh-cli update --dataset etf_share_size
fdh-cli update --dataset etf_sh_cons
fdh-cli update --dataset etf_sz_cons
fdh-cli update --dataset idx_anns

# 指定代码或日期范围
fdh-cli update --dataset fund_daily --symbols 510300.SH --start-date 2024-01-01
fdh-cli update --dataset etf_sh_cons --trade-date 2026-08-14
```

`etf_index` 是静态目录，可用 `fdh-cli update --dataset etf_index` 刷新全表，或通过
`--symbols` 精确刷新一个指数代码。

## SDK

同步与异步 SDK 均提供同名查询：`get_etf_index`、`get_fund_daily`、
`get_fund_adj`、`get_etf_share_size`、`get_etf_sh_cons`、`get_etf_sz_cons`、
`get_idx_anns`；异步版本在名称后追加 `_async`。时间序列查询均支持精确日期及
`start_date` / `end_date` 范围。

## 数据库迁移

已有数据库请手动执行：

```bash
psql "$DATABASE_URL" -f sql/migrations/036_create_etf_extended_datasets.sql
```

程序不会自动执行该迁移。
