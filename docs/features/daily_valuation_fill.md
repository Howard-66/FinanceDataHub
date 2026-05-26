# A 股日度估值缺失补值

## 目标

`daily_basic` 通过 Tushare `daily_basic` 获取日度估值指标，但部分股票、部分指标、部分时间段会出现空值。

当前实现新增一层独立的派生估值存储，不覆盖 `daily_basic` 原始镜像：

- 原始表: `daily_basic`
- 补值表: `processed_daily_valuation_fill`
- 合并视图: `v_daily_basic_enriched`

SDK 默认仍返回 raw 口径；只有显式传 `filled=True` 才会读取 enriched 视图。

## 支持范围

v1 仅覆盖 A 股日频估值。

可补值指标：
- `pe_ttm`
- `pb`
- `ps_ttm`
- `peg`
- `pe`
- `ps`

当前不补值：
- `dv_ratio`
- `dv_ttm`

原因：
- 股息率需要 `dividend` 数据按除权除息日和每股分红口径处理
- 仅靠三大报表不能严谨复原 `daily_basic` 股息率

## 计算口径

时间对齐：
- 财报在 `f_ann_date_time` / `ann_date_time` 当天开始生效
- 设计目标是避免未来函数

市值：
- 优先使用 `daily_basic.total_mv`
- 缺失时回退到 `symbol_daily.close * daily_basic.total_share`

估值：
- `pe_ttm = total_mv / TTM归母净利润`
- `pb = total_mv / 最新已公告归母权益`
- `ps_ttm = total_mv / TTM营业总收入`
- `pe`、`ps` 使用最新已公告年报口径
- `peg = pe_ttm / netprofit_yoy`
- 当 TTM 历史窗口不完整、无法可靠计算时，允许 `pe_ttm = pe`、`ps_ttm = ps`

限制：
- 仅在分母为正时补值
- `peg` 仅在 `pe_ttm` 和 `netprofit_yoy` 同时为正时补值
- 财报单位按元读取，内部转换到 `daily_basic.total_mv` 的万元口径
- TTM fallback 会在 `sources`、`denominator_dates`、`quality_flags` 中明确标记，不与正常 TTM 口径混淆

## CLI 用法

前置数据：

```bash
fdh-cli update --dataset daily_basic
fdh-cli update --dataset income
fdh-cli update --dataset balancesheet
fdh-cli update --dataset fina_indicator
```

执行补值：

```bash
# 增量模式
fdh-cli preprocess run --all --category valuation_fill

# 全量重建
fdh-cli preprocess run --all --category valuation_fill --force
```

补值完成后，建议继续运行估值分位：

```bash
fdh-cli preprocess run --all --category fundamental
```

查看能力和状态：

```bash
fdh-cli preprocess info
fdh-cli preprocess status
```

调度建议顺序：

```text
daily_basic_update
financial_update
valuation_fill_preprocess
fundamental_preprocess
industry_valuation_preprocess
```

## SDK 用法

默认 raw：

```python
basic = fdh.get_daily_basic(
    ['600519.SH'],
    '2024-01-01',
    '2024-12-31',
)
```

读取逐字段补值结果：

```python
filled_basic = fdh.get_daily_basic(
    ['600519.SH'],
    '2024-01-01',
    '2024-12-31',
    filled=True,
)

filled_basic[
    [
        'symbol',
        'time',
        'pe',
        'pe_ttm',
        'pb',
        'ps',
        'ps_ttm',
        'peg',
        'pe_ttm_source',
        'pb_source',
        'valuation_fill_formula_version',
    ]
].tail()
```

`filled=True` 时额外返回：
- `peg`
- `*_source`
- `valuation_fill_sources`
- `valuation_fill_denominator_dates`
- `valuation_fill_quality_flags`
- `valuation_fill_formula_version`

其中 TTM fallback 的典型标记包括：
- `sources["pe_ttm"] = "derived_from_lfy_pe_when_ttm_unavailable"`
- `sources["ps_ttm"] = "derived_from_lfy_ps_when_ttm_unavailable"`
- `quality_flags["pe_ttm"] = "fallback_to_pe_due_to_incomplete_ttm_window"`
- `quality_flags["ps_ttm"] = "fallback_to_ps_due_to_incomplete_ttm_window"`

## 数据表与视图

`processed_daily_valuation_fill` 关键字段：
- `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm`, `peg`, `dv_ratio`, `dv_ttm`
- `sources`
- `denominator_dates`
- `quality_flags`
- `formula_version`

`v_daily_basic_enriched` 合并规则：
- raw 列优先使用 `daily_basic`
- raw 为空时回退到补值表
- `peg` 来自补值层

## 当前边界

- `daily_basic` 不回写补值结果
- 港股、美股未纳入该口径
- 没有财报修订版本历史时，无法完全回放历史修订路径
