# ValueInvesting 宏观周期接入指南

## 目标

`FinanceDataHub` 已经提供中国宏观周期月度主表和行业快照表。`ValueInvesting` 后续应尽量直接消费这些标准化结果，而不是继续在上层重复抓取原始宏观数据、重复计算周期、重复维护行业适配关系。

这份指南聚焦三个落点：

1. 智能选股
2. 个股/宏观页面的数据来源切换
3. qlib 工作流的宏观特征补充

## 推荐接入原则

### 1. 用 `time`，不要用 `observation_time`

`FinanceDataHub` 的 `time` 已经代表“可交易生效月份”，天然适合：

- 智能选股
- 回测
- qlib 训练

不要在 `ValueInvesting` 侧把 `observation_time` 当成可用信号时间，否则会重新引入未来函数。

### 2. 默认消费 `stable_phase`

建议约定：

- 页面展示当前阶段时默认用 `stable_phase`
- 规则诊断或研究对照时才暴露 `raw_phase`

### 3. 行业适配从配置推导改为直接查快照

`industry_config.json` 在 `FinanceDataHub` 中已经落成月度行业快照。下游不需要再维护一套“阶段 -> 行业建议”的平行映射，只需要查询：

- `get_cn_macro_cycle()`
- `get_cn_macro_cycle_industries()`

## 一、宏观 API 与页面改造

### 涉及文件

- `ValueInvesting/src/api/routers/macro.py`
- `ValueInvesting/src/strategy/macro.py`
- `ValueInvesting/web/src/app/macro/page.tsx`

### 当前状态

`ValueInvesting/src/api/routers/macro.py` 仍在：

- 直接抓取原始宏观数据
- 在 API 层重复做 `merge_asof`
- 调用 `src/strategy/macro.py` 重新计算中国宏观周期

### 建议改造

把中国部分改成直接调用 `FinanceDataHub` SDK：

```python
phase_df = await fdh.get_cn_macro_cycle_async(
    start_date="2016-01-01",
    phase_mode="stable",
)

industry_df = await fdh.get_cn_macro_cycle_industries_async(
    start_date="2016-01-01",
    preferred_only=False,
    phase_mode="stable",
)
```

接口映射建议：

- `/api/macro/cn/cycle`
  取最新一条 `phase_df`
- `/api/macro/cn/history-full`
  直接返回 `phase_df`
- `/api/macro/cn/credit-impulse`
  从 `phase_df` 提取 `credit_impulse`
- 宏观页面行业建议
  从 `industry_df` 按最新 `time` 过滤 `matches_stable_phase == true`

建议保留 `src/strategy/macro.py` 作为历史参考或单元测试对照，不再作为线上中国宏观阶段的主计算入口。

## 二、智能选股改造

### 涉及文件

- `ValueInvesting/src/data/screener_service.py`
- `ValueInvesting/web/src/app/screener/page.tsx`

### 当前状态

智能选股已经在消费：

- 行业差异化估值
- F-Score
- qlib 分数

这意味着宏观周期接入非常适合做成“规则筛选前置条件”或“排序增强上下文”。

### 推荐做法

为筛选参数新增两个可选项：

- `use_macro_cycle_filter: bool = False`
- `macro_phase_mode: Literal["stable", "raw"] = "stable"`

处理流程建议：

1. 查询最新宏观阶段：
   `get_cn_macro_cycle_async(phase_mode="stable")`
2. 查询当期优先行业：
   `get_cn_macro_cycle_industries_async(preferred_only=True, phase_mode="stable")`
3. 将候选股票的 `l3_name` 与优先行业集合求交集
4. 对未命中的股票：
   可以直接过滤，或保留但降低规则分

返回字段建议新增：

- `macro_phase`
- `macro_phase_label`
- `macro_effective_time`
- `macro_industry_match`

前端可据此在智能选股页增加：

- 当前宏观阶段标签
- 候选池中“顺周期行业命中率”
- 单只股票的“行业是否匹配当前宏观阶段”提示

## 三、个股分析页改造

### 涉及文件

- `ValueInvesting/src/data/fdh_stock_service.py`
- `ValueInvesting/web/src/app/analysis/[ticker]/page.tsx`

### 推荐补充字段

在个股详情返回中加入：

- `macro_phase`
- `macro_phase_label`
- `macro_effective_time`
- `macro_industry_match`
- `macro_config_cycle`

这样可以让个股页直接展示：

- 当前所处宏观阶段
- 当前行业是否为优先行业
- 行业配置写的是哪个宏观阶段

这类信息和现有的行业估值卡片天然互补。

## 四、qlib 周频面板改造

### 涉及文件

- `ValueInvesting/src/ml/weekly_feature_panel.py`
- `ValueInvesting/src/ml/qlib_pipeline.py`
- `ValueInvesting/scripts/export_qlib_weekly_panel.py`
- `ValueInvesting/scripts/evaluate_qlib_weekly.py`
- `ValueInvesting/scripts/train_qlib_weekly.py`

### 现有优势

`weekly_feature_panel.py` 已经有成熟的 `merge_asof` 框架，当前在做：

- 周频技术面
- 日频/季频基本面
- 行业差异化估值

宏观周期最适合按同样方式并入。

### 建议补充的 FDH 查询

```python
macro_phase = await fdh.get_cn_macro_cycle_async(
    start_date=start_date,
    end_date=end_date,
    phase_mode="stable",
)

macro_industry = await fdh.get_cn_macro_cycle_industries_async(
    start_date=start_date,
    end_date=end_date,
    preferred_only=False,
    phase_mode="stable",
)
```

### 建议新增特征列

连续值：

- `macro_credit_impulse`
- `macro_ppi_yoy`
- `macro_pmi`

阶段编码：

- `macro_phase_y`
- `macro_phase_changed`

one-hot：

- `macro_phase_reflation`
- `macro_phase_recovery`
- `macro_phase_overheat`
- `macro_phase_stagflation`

行业对齐：

- `macro_industry_match`

### 合并建议

1. 先把 `macro_phase` 按 `time` backward merge 到周频面板
2. 再把 `macro_industry` 按 `time + l3_name` 合并
3. 若 `l3_name` 尚未在面板中保留，优先复用现有行业估值合并后的字段

### `qlib_pipeline.py` 建议分组

建议新增两个特征组：

- `macro_cycle_numeric`
  包含 `macro_credit_impulse`, `macro_ppi_yoy`, `macro_pmi`, `macro_phase_y`
- `macro_cycle_flags`
  包含 `macro_phase_changed`、各阶段 one-hot、`macro_industry_match`

归一化建议：

- `macro_cycle_numeric` 可加入标准化候选
- `macro_cycle_flags` 保持原值，不做 robust scale

这样做的好处是，宏观 regime 既能影响模型对收益环境的识别，又不会把布尔状态和连续值混在一个处理策略里。

## 五、测试与验收建议

### 智能选股

- 新增测试：宏观行业过滤开启时，非匹配行业被正确过滤或降权
- 新增测试：`stable/raw` 切换时返回结果有可解释差异

### 宏观页面

- 对比旧接口与新接口在相同区间的阶段结果
- 确认 `history-full` 图线使用 `time` 后没有前视偏差

### qlib

- 新增面板导出测试，确认宏观特征列被写入
- 新增训练 smoke，确认新列不会破坏现有特征收敛流程
- 对比加入宏观特征前后的评估产物，确认没有大面积缺失值和时间错位

## 六、建议的落地顺序

1. 先替换 `macro.py` 路由的数据源，统一宏观页面口径
2. 再给智能选股加 `macro_industry_match`
3. 最后把宏观特征并入 `weekly_feature_panel.py` 和 `qlib_pipeline.py`

这个顺序的好处是：

- 页面和规则先拿到统一口径
- qlib 特征改造放在最后，便于单独评估增量价值

## 对应的 FinanceDataHub 接口

- `fdh.get_cn_macro_cycle(...)`
- `fdh.get_cn_macro_cycle_industries(...)`
- `fdh.get_cn_macro_cycle_async(...)`
- `fdh.get_cn_macro_cycle_industries_async(...)`

相关实现说明见：

- [中国宏观周期预处理](../features/cn_macro_cycle_preprocessing.md)
