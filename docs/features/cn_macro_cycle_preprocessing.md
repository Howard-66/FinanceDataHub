# 中国宏观周期预处理

## 背景

`ValueInvesting` 项目已经基于 M2、GDP、PPI、PMI 计算中国宏观周期，并在页面中展示了历史阶段变化。为了让智能选股、回测和 qlib 工作流都消费同一份标准化数据，这套逻辑已经前移到 `FinanceDataHub`，形成可调度、可查询、可复用的月度预处理。

本次落地后，`FinanceDataHub` 不再只保存原始宏观数据，也会直接提供：

- 月度中国宏观阶段主表
- 与申万三级行业配置对齐的行业快照表
- CLI、Scheduler、SDK 的统一访问入口

## 数据来源与计算口径

### 原始数据表

- `cn_m`
  使用 `m2_yoy`
- `cn_ppi`
  使用 `ppi_yoy`
- `cn_pmi`
  优先使用 `pmi010000`，缺失时回退到 `pmi030000`
- `cn_gdp`
  使用 `gdp_yoy`

### 时间处理规则

- `observation_time`
  原始观测月份，统一标准化到月末 `15:00 Asia/Shanghai`
- `time`
  可交易生效月份，统一采用 `observation_time + 1个月`

这样做的原因是库内没有可靠的发布日期字段，宏观预处理默认按“整体滞后 1 个月生效”近似处理，避免回测和训练时未来函数泄漏。

### 阶段判定规则

核心变量：

- `credit_impulse = m2_yoy - gdp_yoy`
- `ppi_yoy`
- `pmi`

阶段枚举：

- `REFLATION`
- `RECOVERY`
- `OVERHEAT`
- `STAGFLATION`

同时保留两条轨道：

- `raw_phase`
  完全复刻 `ValueInvesting/src/strategy/macro.py` 的判定逻辑
- `stable_phase`
  新阶段连续 2 个生效月确认后才切换，用于降低月度抖动

默认建议下游消费 `stable_phase`，诊断或对照时再看 `raw_phase`。

## 预处理产物

### 1. `processed_cn_macro_cycle_phase`

月度主表，保存宏观阶段真值与可交易生效时间。

核心字段：

- `time`
- `observation_time`
- `m2_yoy`
- `gdp_yoy`
- `ppi_yoy`
- `pmi`
- `credit_impulse`
- `raw_phase`
- `stable_phase`
- `raw_phase_changed`
- `stable_phase_changed`
- `processed_at`

典型用途：

- 宏观页面阶段历史曲线
- 回测中的 regime filter
- qlib 的宏观上下文特征

### 2. `processed_cn_macro_cycle_industry`

按月生成的申万三级行业快照，按 `industry_config.json` 判断行业是否匹配当期宏观阶段。

核心字段：

- `time`
- `observation_time`
- `l1_code`, `l1_name`
- `l2_code`, `l2_name`
- `l3_code`, `l3_name`
- `config_macro_cycle`
- `core_indicator`
- `ref_indicator`
- `logic`
- `fscore_exemptions`
- `is_present_in_sw_member`
- `matches_raw_phase`
- `matches_stable_phase`
- `processed_at`

典型用途：

- 智能选股候选池按宏观阶段过滤
- 行业轮动回测
- 宏观页面行业建议展示

## 行业配置治理

`industry_config.json` 现在承担双重职责：

1. 行业差异化估值预处理
2. 宏观周期行业快照预处理

因此建议把它视为“行业策略元数据”而不是单纯估值配置。任何配置变更后，都应至少重跑：

- `industry_valuation`
- `macro_cycle`

当前预处理会校验数据库中已有的 `sw_industry_member.l3_name` 是否全部被配置覆盖；如果存在缺口，会直接报错，避免下游静默丢行业。

## CLI 与 SDK

### CLI

```bash
# 运行中国宏观周期预处理
fdh-cli preprocess run --category macro_cycle

# 查看预处理状态
fdh-cli preprocess show-status
```

### SDK

```python
from finance_data_hub import FinanceDataHub

fdh = FinanceDataHub()

phase_df = fdh.get_cn_macro_cycle(
    start_date="2020-01-01",
    phase_mode="stable",
)

industry_df = fdh.get_cn_macro_cycle_industries(
    start_date="2020-01-01",
    preferred_only=True,
    phase_mode="stable",
)
```

查询约定：

- `phase_mode`
  可选 `stable` 或 `raw`
- `preferred_only=True`
  只返回匹配当前阶段的行业快照

## 定时任务配置

宏观相关调度建议如下：

- `macro_update`
  每月 15 号 `08:00` 拉取原始宏观数据
- `macro_cycle_preprocess`
  每月 15 号 `08:20` 运行宏观周期预处理

依赖关系：

- `macro_cycle_preprocess -> macro_update`
- `macro_cycle_preprocess -> sw_member_update`

这样可以保证宏观原始数据和申万行业映射都已经更新完，再生成行业快照。

## 下游接入建议

### 智能选股

- 默认使用 `stable_phase`
- 使用 `processed_cn_macro_cycle_industry.matches_stable_phase` 过滤候选行业
- 估值与 F-Score 继续使用现有日频/季频预处理

### 回测与训练

- 使用 `time` 作为生效时间，不要直接用 `observation_time`
- 月频宏观信号在日频或周频样本中按时间向后填充
- 宏观阶段切换标记可以作为 regime shift 特征或样本切片条件

### 页面展示

- 宏观页面直接读取 `processed_cn_macro_cycle_phase`
- 阶段对应行业建议直接读取 `processed_cn_macro_cycle_industry`
- 阶段 label/color 等展示映射建议由前端或 API 层常量维护，不冗余写入数据库

## 相关文档

- [定时下载与数据预处理设计](./SchedulerPreprocessing.md)
- [行业差异化估值指标预处理方案](./distinct_industry_valuation.md)
- [ValueInvesting 宏观周期接入指南](../guides/valueinvesting_macro_cycle_integration_guide.md)
