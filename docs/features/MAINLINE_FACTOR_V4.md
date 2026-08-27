# 主线因子 v4：恢复说明、数据契约与运行手册

> 恢复依据：FinanceDataHub 提交 `e69b1d7`（2026-08-27）及其测试，而非从
> 已删除文稿逐字还原。本文是当前 `factor_version=4` 的实现说明；v1--v3
> 数据、快照和回测记录不被覆盖。

## 1. 目标与边界

v4 为 AlphaLine 提供严格的时点（PIT）行业主线输入：截至观察日实际已知的
股票、行业、ETF、基金披露和交易事实，最早在下一个交易日才可使用。行业范围
固定为已发布的 SW2021 二级行业（124 个）。

它不是自动交易系统。FinanceDataHub 只发布 `ready` / `blocked` 的数据快照；
策略层仅能读取 `ready` 快照，组合构建与回测由 ValueInvesting 的 AlphaLine v4
负责。

## 2. 相对于 v3 的优化

| 层级 | v4 实施 |
| --- | --- |
| 行业相对强弱 | 改以官方行业指数的 20 / 60 / 120 日收益减基准收益计算，不再以个股平均收益替代。 |
| 强势股广度 | 强势股为当日全市场 60 日收益排名前 20% 且 `amount_ratio_20_60 > 1.20` 的股票；行业内聚合数量与占比。 |
| 多维动量 | 新增 20 日相对强弱斜率、60 日风险调整动量、信息比率、路径效率、60 日单日收益尾部代理。 |
| 拥挤度 | `crowding_input` 保留披露浓度输入；`crowding_score` 只应在具备完整 504 日 PIT 行业自身历史时写入，不能把横截面基金集中度当作奖励。 |
| ETF 映射 | 以已审核的基准映射为前提；默认只使用从历史指数权重得到的精确 L2 暴露。L1 / 主题代理必须有额外、已审核的 PIT 证据，代码不会猜测代理。 |
| 发布门禁 | ETF 工具数量不足不再阻塞数据发布：无合格工具的有效快照会发布，策略应转为现金，不能继续持有旧 ETF。 |
| 领先滞后 | 训练样本改为以时点 `t` 特征预测 `t+20` 日相对收益，避免把已实现收益当作预测目标。该信号在默认策略中为 shadow-only。 |

当前公式身份由 `MAINLINE_FORMULA_HASH` 固化；修改口径、阈值或映射规则时必须创建
新的因子版本和新的公式哈希，不能重写既有 `ready` 快照。

## 3. ETF 历史基准映射：人工审核后的处理

`045_add_mainline_etf_mapping_v3.sql` 创建的
`mainline_etf_benchmark_history` 是历史基准映射的事实表。只有同时满足以下条件的行
可以进入 v4 ETF 白名单：

- `mapping_status = 'mapped'`；
- `review_status = 'approved'`；
- `usable_from_trade_date <= trade_date`，且未过 `usable_to_trade_date`；
- 所需价格、份额、规模与暴露数据完整，并通过流动性、规模、折溢价、跟踪误差检查。

因此，已人工审核的原 `mapping_pending` 记录应保留其审核人、审核时间、证据链接和
真实可用日；不要再次用当前 `etf_basic.index_code` 回填历史，也不要把待审行直接
用于回测。`ambiguous_multisector` 和 `not_applicable` 都是有效的排除结论，不应为了
增加候选数改为 `mapped`。

v4 另有 `mainline_etf_strategy_mapping_history`，只处理“已具备有效历史基准映射，
但策略工具要采用 L1 / 主题代理”的情形。精确 L2 映射不需要插入该表：预处理会从
`processed_mainline_etf_exposure_summary` 自动生成 `mapping_level='exact_l2'` 和证据。

代理映射必须具有重叠约束之外的人工证据，且以可用日期而非事后成分日期生效：

```sql
INSERT INTO mainline_etf_strategy_mapping_history (
  ts_code, effective_from_date, usable_from_trade_date, mapping_level,
  target_l2_codes, target_l1_code, target_coverage, non_target_l1_exposure,
  exposure_vector, evidence_reference, reviewed_by
) VALUES (
  '<ETF代码>', '<实际生效日>', '<信息实际可用日>', 'l1_proxy',
  ARRAY['<目标L2>'], '<目标L1>', 0.70, 0.20,
  '{"<目标L2>":0.70}', '<公告/指数文件编号或链接>', '<审核人>'
);
```

这不是把“目录名称相似”转换成代理关系的入口。`mapping_level` 只允许
`exact_l2`、`l1_proxy`、`theme_proxy`；同一 ETF 的可用区间不可重叠。

## 4. 结构迁移与重建

若 045 / 046 已经执行，且 `mapping_pending` 已完成人工审核，只需在其后执行 047。
迁移可重复执行；不删除旧版本数据。

```bash
export PSQL=/opt/homebrew/opt/libpq/bin/psql

"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f sql/migrations/045_add_mainline_etf_mapping_v3.sql
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f sql/migrations/046_auto_approve_mainline_etf_basic_mappings.sql
"$PSQL" "$DATABASE_URL" -v ON_ERROR_STOP=1 \
  -f sql/migrations/047_add_mainline_factor_v4.sql
```

在已人工修订映射的环境中，先核对审核记录，再决定是否运行 046；046 只会处理仍为
`mapping_pending + pending` 且同时具备目录基准代码和成立/上市日期的行，不能替代
人工历史证据。

完整重建应使用最近一个已完成交易日：

```bash
export MAINLINE_END_DATE=2026-08-26  # 替换为实际最近完成交易日

fdh-cli preprocess run --category mainline --stage rebuild --all \
  --start-date 2012-01-01 --end-date "$MAINLINE_END_DATE" --force
fdh-cli preprocess status --mainline
```

`rebuild` 顺序固定为：PIT 桥接 → `stock` → `market` → ETF / 暴露 →
`crowding` → `industry` → `leadlag` → Gate 0 / `publish` → 索引和压缩策略。
股票、市场和行业的领先滞后训练会读取自 2008-01-01 起的预热窗口；同一版本、日期和
公式哈希可从未完成或失败阶段续跑。

若只修改了审核映射或分层代理证据，可局部重建 ETF、暴露与发布，再由策略端重新物化
受影响月末快照：

```bash
fdh-cli preprocess run --category mainline --stage etf,exposure,publish --all \
  --start-date <最早受影响的可用日> --end-date "$MAINLINE_END_DATE" --force
```

## 5. 验收查询

```sql
-- 人工审核队列应为空，或仅含尚未决定且不会入选的记录。
SELECT ts_code, benchmark_index_code, usable_from_trade_date,
       mapping_status, review_status, source_name, evidence_reference
FROM mainline_etf_benchmark_history
WHERE mapping_status = 'mapping_pending' OR review_status = 'pending'
ORDER BY ts_code, usable_from_trade_date;

-- v4 每个可完成交易日应有 124 个已发布 SW2021 L2 行业。
SELECT trade_date, COUNT(*) AS l2_count
FROM processed_mainline_industry_daily
WHERE factor_version = 4
GROUP BY trade_date
HAVING COUNT(*) <> 124
ORDER BY trade_date;

-- 映射状态、可执行工具数和 Gate 0 是分别观察的指标。
SELECT partition_date, status, details -> 'mapping_status_counts' AS mapping_status_counts,
       details ->> 'executable_candidate_count' AS executable_candidate_count,
       blocker_reasons
FROM processed_mainline_data_status
WHERE factor_version = 4 AND dataset = 'etf_daily'
ORDER BY partition_date DESC
LIMIT 30;

SELECT status, MIN(as_of_trade_date), MAX(as_of_trade_date), COUNT(*)
FROM processed_mainline_snapshot_manifest
WHERE factor_version = 4
GROUP BY status
ORDER BY status;
```

只有 `processed_mainline_snapshot_manifest.status='ready'` 的观察日可交给正式的
AlphaLine v4 快照或回测。`blocked` 日期必须先修复原始输入、PIT 映射或覆盖率，不能
用当前映射、零值或降低门槛绕过。

## 6. 面向 AlphaLine 的输出契约

策略端以观察日和执行日查询 `market_daily`、`industry_daily`、`etf_daily` 及
`leadlag_score_monthly`，均带 `factor_version=4`。`usable_from_trade_date` 是策略可
使用数据的下界；查询不得读取未来可用的行。

ETF 日表除传统交易质量字段外，必须透传：`benchmark_mapping_status`、来源、置信度、
审核状态、`mapping_level`、`target_coverage`、`non_target_l1_exposure`、
`exposure_vector` 和 `mapping_evidence`。这使策略选择、组合和之后的回放都能说明
“为什么这只 ETF 当时可以或不可以被使用”。

后续操作见 ValueInvesting 的
[`AlphaLine-v4-量化主线策略与运行手册.md`](../../../ValueInvesting/docs/1-主线量化方案/AlphaLine-v4-量化主线策略与运行手册.md)。
