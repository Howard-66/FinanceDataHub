<!-- OPENSPEC:START -->
# OpenSpec Instructions

These instructions are for AI assistants working in this project.

Always open `@/openspec/AGENTS.md` when the request:
- Mentions planning or proposals (words like proposal, spec, change, plan)
- Introduces new capabilities, breaking changes, architecture shifts, or big performance/security work
- Sounds ambiguous and you need the authoritative spec before coding

Use `@/openspec/AGENTS.md` to learn:
- How to create and apply change proposals
- Spec format and conventions
- Project structure and guidelines

Keep this managed block so 'openspec update' can refresh the instructions.

<!-- OPENSPEC:END -->

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

**FinanceDataHub** 是一个综合性金融数据服务中心，旨在为行情显示、量化分析、投资研究和策略回测提供统一、可靠、高性能的数据基础。

### 核心原则
- **开放与扩展性**: 架构设计开放，轻松接入新的数据源和存储引擎
- **服务解耦**: 严格分离数据获取、存储、访问和流处理
- **性能优先**: 采用列式存储和内存计算技术处理高频金融数据
- **开发者友好**: 简洁、直观的API，专注于数据使用而非获取
- **数据一致性**: 明确的数据更新机制（增量/全量）
- **可观测性**: 完整的日志记录和性能监控

## 当前开发状态

**阶段**: Phase 3 已完成 ✅
- ✅ Phase 1 - 环境搭建与配置管理（已完成）
- ✅ Phase 2 - 核心批处理流程（已完成）
- ✅ Phase 2.5 - 高周期数据聚合（已完成）
- ✅ Phase 3 - 数据访问与查询层（已完成）
- 📋 下一阶段：Phase 4 - ETL与流式处理

### Phase 3 完成情况

**已实现功能**:
- ✅ DataOperations 查询方法（5个）
- ✅ SDK 查询接口（10个方法对 = 5对同步/异步）
- ✅ SmartRouter 智能路由集成
- ✅ 路由决策日志记录
- ✅ 数据新鲜度检查
- ✅ 自动数据库初始化
- ✅ Jupyter Notebook 兼容
- ✅ 完整文档和测试

**支持的数据类型**:
- 日线数据、分钟数据、每日基本面、复权因子、股票基本信息
- 周线/月线数据（自动聚合）
- 中国GDP宏观经济数据（季度）
- 中国PPI宏观经济数据（月度）
- 中国货币供应量M0/M1/M2数据（月度）
- 中国PMI采购经理人指数数据（月度）
- 大盘指数每日指标数据（日度，上证综指、深证成指、上证50、中证500等）
- 上市公司财务指标数据（每股收益、ROE、资产负债率等，季度）
- 上市公司现金流量表数据（经营活动、投资活动、筹资活动现金流量，季度）
- 上市公司资产负债表数据（资产、负债、股东权益，季度）
- 上市公司利润表数据（收入、成本和利润，季度）

## 高层架构设计

系统采用分层架构设计，包含四个主要层：

```
应用层 (Qlib, FinRL, Jupyter, WebSocket 看板)
        ↓
数据访问SDK (Python SDK + 智能路由)
        ↓
存储层 (Hot: PostgreSQL+TimescaleDB | Cold: Parquet+DuckDB)
        ↓
核心服务层 (CLI工具 + 调度器 + 智能源路由)
        ↓
数据源层 (Tushare批量 | XTQuant流式)
        ↓
流处理总线 (Redis Pub/Sub)
```

### 关键组件说明

**1. Provider Layer（数据源层）**
- **适配器模式**: 统一不同数据源的接口差异
- **TushareProvider**: 批量数据适配器，负责历史数据获取
- **XTQuantProvider**: 流式数据适配器（通过HTTP API客户端连接xtquant_helper微服务）
- **标准化**: 所有Provider返回统一格式的DataFrame

**2. Storage Layer（存储层 - 冷热分离）**
- **主存储**: PostgreSQL + TimescaleDB (OLTP)
  - 表结构: `asset_basic`, `symbol_daily` 等
  - 角色: 数据真实来源，负责首次写入、修正、增量更新
- **分析存储**: Parquet + DuckDB (OLAP)
  - 分区策略: 按symbol或按日期分区
  - 角色: AI训练和复杂分析优化的只读副本

**3. Core Service Layer（核心服务层）**
- **CLI工具** (`fdh-cli`):
  - `update`: 从数据源拉取数据（集成智能数据源路由）
  - `etl`: 将主数据库同步到分析存储
  - `status`: 监控数据完整性
- **智能数据源路由**: 基于 `sources.yml` 配置的提供商选择与故障转移
- **调度器**: APScheduler或Crontab，支持定时数据更新

**4. Streaming Bus（流式处理总线）**
- **Redis Pub/Sub**: 轻量级消息中间件
- **实时数据通道**: XTQuantProvider连接xtquant_helper WebSocket获取实时行情
- **归档服务**: 订阅流式数据并批量持久化到TimescaleDB

**5. Data Access SDK（数据访问SDK）**
- **FinanceDataHub类**: 提供同步和异步API
- **智能路由**: 自动选择PostgreSQL或DuckDB作为查询后端
- **开发者接口**: 简洁的数据获取接口

## 技术栈

| 组件 | 技术选型 |
|------|----------|
| 语言 | Python 3.10+ |
| 配置 | Pydantic + .env |
| CLI框架 | Typer或Click |
| 主数据库 | PostgreSQL 16 + TimescaleDB |
| 分析引擎 | DuckDB |
| 文件格式 | Parquet (Zstd压缩) |
| 调度 | APScheduler或Crontab |
| 流处理 | Redis Pub/Sub |
| ORM | SQLAlchemy (Core) + asyncpg |
| 日志 | Loguru |
| 部署 | Docker Compose |
| 微服务 | FastAPI (xtquant_helper) |

## 关键配置

**环境变量** (`.env`):
```env
TUSHARE_TOKEN=your_tushare_token
XTQUANT_API_URL=http://192.168.1.100:8000
DATABASE_URL=postgresql://user:pass@localhost:5432/financedatahub
REDIS_URL=redis://localhost:6379
```

**数据源配置** (`sources.yml`):
```yaml
providers:
  tushare:
    token: "${TUSHARE_TOKEN}"
  xtquant:
    api_url: "${XTQUANT_API_URL}"

routing_strategy:
  stock:
    daily: [tushare, xtquant]
    minute_1: [xtquant]
    tick: [xtquant]

  # 宏观经济数据路由
  macro:
    gdp:
      providers: [tushare]
      fallback: false
    ppi:
      providers: [tushare]
      fallback: false
    m:
      providers: [tushare]
      fallback: false
    pmi:
      providers: [tushare]
      fallback: false

  # 指数数据路由
  index:
    dailybasic:
      providers: [tushare]
      fallback: false
```

## 实施计划

### Phase 1: 环境与配置 ✅ 已完成
- [x] 编写 `docker-compose.yml` 部署 PG+TimescaleDB 和 Redis
- [x] 实现基于 Pydantic 的配置模块 (`config.py`)
- [x] 搭建 `fdh-cli` 基本框架

### Phase 2: 核心批处理流程 ✅ 已完成
- [x] 实现 `TushareProvider`（直接API调用）
- [x] 实现 `XTQuantProvider`（作为 `xtquant_helper` 的HTTP API客户端）
- [x] 定义并实现 `sources.yml` 配置加载及智能数据源路由逻辑
- [x] 实现 `fdh-cli update` 命令
- [x] 实现 `fdh-cli etl` 命令

### Phase 3: 数据访问与查询 ✅ 已完成
- [x] 封装 `FinanceDataHub` SDK 类
- [x] 实现基于 PostgreSQL 的基础查询接口（5种数据类型）
- [x] 在 SDK 中加入"智能路由"逻辑和异步接口
- [x] 实现同步/异步双接口（10个方法对）
- [x] 集成 SmartRouter 智能路由
- [x] 添加路由决策日志记录
- [x] 实现数据新鲜度检查功能
- [x] 支持 Jupyter Notebook 环境
- [x] 编写完整文档和测试

### Phase 4: 流式处理与高级特性 📋 规划中
- [ ] 在 `xtquant_helper` 服务中增加 WebSocket 接口
- [ ] 实现流式 `Provider` 连接 WebSocket
- [ ] 实现数据到 Redis Pub/Sub 的发布
- [ ] 编写 `Archiver` 服务持久化流式数据
- [ ] 实现完整ETL（PostgreSQL → Parquet + DuckDB）
- [ ] 对接 Qlib/FinRL 数据格式的导出功能

### Phase 5: 测试与部署 📋 规划中
- [ ] 编写单元测试（使用 `pytest` 和 `mock`）
- [ ] 完善 Dockerfile 和部署脚本
- [ ] 性能测试与优化

## XTQuant 集成注意事项

- XTQuant 仅支持 Windows 且依赖 QMT
- 通过 HTTP API 客户端模式集成：`XTQuantProvider` → `xtquant_helper` 微服务
- `xtquant_helper` 必须提供：
  1. REST API 用于批量数据请求
  2. WebSocket 接口用于实时行情推送
- 微服务地址在 `sources.yml` 中配置

## 数据标准化约定

**列名统一**:
- 标准列: `time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adj_factor`
- 时间格式: ISO 8601 或 Pandas Timestamp
- 数值类型: 浮点数（价格、成交量）和整数（股票代码）

**Symbol格式**:
- 格式: `<code>.<exchange>`
- 示例: `600519.SH`（贵州茅台沪市）、`000858.SZ`（五粮液深市）

## 常用开发命令

（实施后将在此添加实际命令）

```bash
# 构建和运行
# TODO: 添加 docker-compose, CLI 等命令

# 测试
# TODO: 添加 pytest 命令

# 代码质量
# TODO: 添加 black, flake8, mypy 等命令
```

## 重要设计决策

1. **智能路由**: 通过 `sources.yml` 配置驱动数据源选择，支持故障转移
2. **冷热分离**: PostgreSQL作为源数据存储，Parquet+DuckDB作为分析存储
3. **微服务架构**: XTQuant作为独立微服务，通过HTTP API集成
4. **事件驱动**: Redis Pub/Sub作为实时数据流总线
5. **配置即代码**: 使用Pydantic实现类型安全的配置管理

## API 使用示例

### 在 Jupyter Notebook 中使用（推荐）

```python
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

# 初始化
settings = get_settings()
fdh = FinanceDataHub(
    settings=settings,
    backend="postgresql",
    router_config_path="sources.yml"  # 可选
)

# 直接使用 await（推荐方式）
daily_data = await fdh.get_daily_async(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')
print(f"日线数据: {len(daily_data)} 条记录")
print(daily_data.head())

# 分钟数据查询
minute_data = await fdh.get_minute_async(
    ['600519.SH'],
    '2024-11-01',
    '2024-11-30',
    'minute_5'
)
print(f"5分钟数据: {len(minute_data)} 条记录")

# 每日基本面查询
basic_data = await fdh.get_daily_basic_async(
    ['600519.SH'],
    '2024-01-01',
    '2024-12-31'
)
print(f"每日基本面: {len(basic_data)} 条记录")

# 复权因子查询
adj_data = await fdh.get_adj_factor_async(
    ['600519.SH'],
    '2020-01-01',
    '2024-12-31'
)
print(f"复权因子: {len(adj_data)} 条记录")

# 股票基本信息查询
info = await fdh.get_basic_async(['600519.SH', '000858.SZ'])
print(f"股票信息: {len(info)} 条记录")

# 高周期数据查询（自动聚合）
weekly = await fdh.get_weekly_async(['600519.SH'], '2024-01-01', '2024-12-31')
monthly = await fdh.get_monthly_async(['600519.SH'], '2024-01-01', '2024-12-31')

# GDP宏观经济数据查询（使用季度末日期，如 2024-03-31 表示 2024Q1）
gdp_data = await fdh.get_cn_gdp_async('2020-03-31', '2024-12-31')
print(f"GDP数据: {len(gdp_data)} 条记录")
print(gdp_data.head())

# PPI宏观经济数据查询（使用月份末日期，如 2024-01-31 表示 2024年1月）
ppi_data = await fdh.get_cn_ppi_async('2020-01-31', '2024-12-31')
print(f"PPI数据: {len(ppi_data)} 条记录")
print(ppi_data.head())

# 货币供应量数据查询（使用月份末日期，如 2024-01-31 表示 2024年1月）
m_data = await fdh.get_cn_m_async('2020-01-31', '2024-12-31')
print(f"货币供应量数据: {len(m_data)} 条记录")
print(m_data.head())

# PMI采购经理人指数数据查询（使用月份末日期，如 2024-01-31 表示 2024年1月）
pmi_data = await fdh.get_cn_pmi_async('2020-01-31', '2024-12-31')
print(f"PMI数据: {len(pmi_data)} 条记录")
print(pmi_data[['time', 'pmi010000', 'pmi020100', 'pmi030000']])

# 大盘指数每日指标数据查询
# 支持的指数：上证综指(000001.SH)、深证成指(399001.SZ)、上证50(000016.SH)、中证500(000905.SH)、中小板指(399005.SZ)、创业板指(399006.SZ)
index_data = await fdh.get_index_dailybasic_async('000001.SH', '2024-01-01', '2024-12-31')
print(f"上证综指每日指标: {len(index_data)} 条记录")
print(index_data[['trade_date', 'total_mv', 'pe', 'pb', 'turnover_rate']])

# 上市公司财务指标数据查询（使用报告期日期，如 2024-03-31 表示 2024Q1）
fina_data = await fdh.get_fina_indicator_async('600519.SH', '2020-03-31', '2024-12-31')
print(f"贵州茅台财务指标: {len(fina_data)} 条记录")
print(fina_data[['ts_code', 'end_date', 'eps', 'roe', 'debt_to_assets', 'turn_days']].head())

# 上市公司现金流量表数据查询（使用报告期日期，如 2024-03-31 表示 2024Q1）
cashflow_data = await fdh.get_cashflow_async('600519.SH', '2020-03-31', '2024-12-31')
print(f"贵州茅台现金流量表: {len(cashflow_data)} 条记录")
print(cashflow_data[['ts_code', 'end_date', 'net_profit', '经营活动产生的现金流量净额', '投资活动产生的现金流量净额', '筹资活动产生的现金流量净额']].head())

# 上市公司资产负债表数据查询（使用报告期日期，如 2024-03-31 表示 2024Q1）
balancesheet_data = await fdh.get_balancesheet_async('600519.SH', '2020-03-31', '2024-12-31')
print(f"贵州茅台资产负债表: {len(balancesheet_data)} 条记录")
print(balancesheet_data[['ts_code', 'end_date', 'total_assets', 'total_liab', 'total_hldr_eqy_exc_min_int']].head())

# 上市公司利润表数据查询（使用报告期日期，如 2024-03-31 表示 2024Q1）
income_data = await fdh.get_income_async('600519.SH', '2020-03-31', '2024-12-31')
print(f"贵州茅台利润表: {len(income_data)} 条记录")
print(income_data[['ts_code', 'end_date', 'total_revenue', 'n_income', 'ebit']].head())

# 关闭连接
await fdh.close()
```

### 在普通 Python 脚本中使用

```python
import asyncio
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

settings = get_settings()
fdh = FinanceDataHub(settings, backend="postgresql")

# 方式1: 使用同步方法（自动处理事件循环）
daily_data = fdh.get_daily(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')
print(f"日线数据: {len(daily_data)} 条记录")

# 方式2: 使用异步方法
async def get_data():
    daily = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
    minute = await fdh.get_minute_async(['600519.SH'], '2024-11-01', '2024-11-30', 'minute_5')
    await fdh.close()
    return daily, minute

daily_data, minute_data = asyncio.run(get_data())
print(f"日线数据: {len(daily_data)} 条")
print(f"分钟数据: {len(minute_data)} 条")
```

### 数据新鲜度检查

```python
# 检查数据新鲜度
freshness = await fdh.check_data_freshness(['600519.SH'], 'daily')
print(f"可用提供商: {freshness['available_providers']}")
print(f"建议: {freshness['recommendation']}")
```

### 支持的数据类型

| 数据类型 | 方法 | 参数说明 |
|----------|------|---------|
| 日线 | `get_daily()` / `get_daily_async()` | symbols, start_date, end_date |
| 分钟 | `get_minute()` / `get_minute_async()` | symbols, start_date, end_date, frequency |
| 每日基本面 | `get_daily_basic()` / `get_daily_basic_async()` | symbols, start_date, end_date |
| 复权因子 | `get_adj_factor()` / `get_adj_factor_async()` | symbols, start_date, end_date |
| 基本信息 | `get_basic()` / `get_basic_async()` | symbols (可选，None表示所有) |
| 周线 | `get_weekly()` / `get_weekly_async()` | symbols, start_date, end_date |
| 月线 | `get_monthly()` / `get_monthly_async()` | symbols, start_date, end_date |
| GDP | `get_cn_gdp()` / `get_cn_gdp_async()` | start_date, end_date (季度末日期) |
| PPI | `get_cn_ppi()` / `get_cn_ppi_async()` | start_date, end_date (月份末日期) |
| 货币供应量 | `get_cn_m()` / `get_cn_m_async()` | start_date, end_date (月份末日期) |
| PMI | `get_cn_pmi()` / `get_cn_pmi_async()` | start_date, end_date (月份末日期) |
| 指数每日指标 | `get_index_dailybasic()` / `get_index_dailybasic_async()` | ts_code, start_date, end_date |
| 财务指标 | `get_fina_indicator()` / `get_fina_indicator_async()` | ts_code, start_date, end_date (报告期) |
| 现金流量表 | `get_cashflow()` / `get_cashflow_async()` | ts_code, start_date, end_date (报告期) |
| 资产负债表 | `get_balancesheet()` / `get_balancesheet_async()` | ts_code, start_date, end_date (报告期) |
| 利润表 | `get_income()` / `get_income_async()` | ts_code, start_date, end_date (报告期) |

**GDP数据说明**:
- 日期格式使用季度末日期，如 `2024-03-31` 表示 2024Q1，`2024-06-30` 表示 2024Q2
- 返回字段: `time`, `quarter`, `gdp`, `gdp_yoy`, `pi`, `pi_yoy`, `si`, `si_yoy`, `ti`, `ti_yoy`

**PPI数据说明**:
- 日期格式使用月份末日期，如 `2024-01-31` 表示 2024年1月，`2024-12-31` 表示 2024年12月
- 返回字段: `time`, `month`, `ppi_yoy`, `ppi_mp_yoy`, `ppi_cg_yoy` 等33个指标

**货币供应量数据说明**:
- 日期格式使用月份末日期，如 `2024-01-31` 表示 2024年1月，`2024-12-31` 表示 2024年12月
- 返回字段: `time`, `month`, `m0`, `m0_yoy`, `m0_mom`, `m1`, `m1_yoy`, `m1_mom`, `m2`, `m2_yoy`, `m2_mom`

**PMI数据说明**:
- 日期格式使用月份末日期，如 `2024-01-31` 表示 2024年1月，`2024-12-31` 表示 2024年12月
- 返回字段: `time`, `month` 及32个PMI指标字段
- 主要指标包括:
  - `pmi010000`: 制造业PMI
  - `pmi020100`: 非制造业PMI:商务活动
  - `pmi030000`: 中国综合PMI:产出指数
  - 以及企业规模、生产指数、新订单、从业人员等细分指标

**指数每日指标数据说明**:
- 日期格式使用交易日日期，如 `2024-01-02` 表示 2024年1月2日
- 支持的指数代码:
  - `000001.SH`: 上证综指
  - `399001.SZ`: 深证成指
  - `000016.SH`: 上证50
  - `000905.SH`: 中证500
  - `399005.SZ`: 中小板指
  - `399006.SZ`: 创业板指
- 返回字段: `ts_code`, `trade_date`, `total_mv`, `float_mv`, `total_share`, `float_share`, `free_share`, `turnover_rate`, `turnover_rate_f`, `pe`, `pe_ttm`, `pb`

**财务指标数据说明**:
- 日期格式使用报告期日期，如 `2024-03-31` 表示 2024Q1，`2024-06-30` 表示 2024Q2
- 财务数据为季度数据，通常在财报发布后更新
- 支持按股票代码查询，如 `get_fina_indicator('600519.SH', '2020-03-31', '2024-12-31')`
- 返回字段包含90+个财务指标，包括:
  - 每股收益类: `eps`, `dt_eps`, `diluted2_eps`
  - 盈利能力: `roe`, `roe_waa`, `roa`, `roic`, `netprofit_margin`, `grossprofit_margin`
  - 偿债能力: `current_ratio`, `quick_ratio`, `debt_to_assets`
  - 运营效率: `ar_turn`, `ca_turn`, `assets_turn`, `turn_days`
  - 同比数据: `basic_eps_yoy`, `roe_yoy`, `netprofit_yoy` 等

**现金流量表数据说明**:
- 日期格式使用报告期日期，如 `2024-03-31` 表示 2024Q1，`2024-06-30` 表示 2024Q2
- 现金流量表数据为季度数据，通常在财报发布后更新
- 支持按股票代码查询，如 `get_cashflow('600519.SH', '2020-03-31', '2024-12-31')`
- 返回字段包含100+个现金流量指标，包括:
  - 经营现金流: `net_profit`(净利润), `c_rec_sup`(应收款项预付款), `c_pay_sup`(应付款项), `operate_cash_flow`(经营活动产生的现金流量净额)
  - 投资现金流: `invest_cash_flow`(投资活动产生的现金流量净额), `固定资产投资`(固定资产等)
  - 筹资现金流: `finan_cash_flow`(筹资活动产生的现金流量净额), `cash_received`(借款收到的现金), `cash_paid`(偿还债务支付的现金)
  - 汇率变动: `exchange_rate_change`(汇率变动对现金的影响)
  - 其他: `cash_change`(现金及现金等价物净增加额), `begin_cash`(期初现金及现金等价物余额), `end_cash`(期末现金及现金等价物余额)

**资产负债表数据说明**:
- 日期格式使用报告期日期，如 `2024-03-31` 表示 2024Q1，`2024-06-30` 表示 2024Q2
- 资产负债表数据为季度数据，通常在财报发布后更新
- 支持按股票代码查询，如 `get_balancesheet('600519.SH', '2020-03-31', '2024-12-31')`
- 返回字段包含150+个资产负债表指标，包括:
  - 流动资产: `money_cap`(货币资金), `trad_asset`(交易性金融资产), `notes_receiv`(应收票据), `accounts_receiv`(应收账款), `inventories`(存货), `total_cur_assets`(流动资产合计)
  - 非流动资产: `fix_assets`(固定资产), `intan_assets`(无形资产), `lt_eqt_invest`(长期股权投资), `total_nca`(非流动资产合计), `total_assets`(资产总计)
  - 流动负债: `st_borr`(短期借款), `acct_payable`(应付账款), `payroll_payable`(应付职工薪酬), `taxes_payable`(应交税费), `total_cur_liab`(流动负债合计)
  - 非流动负债: `lt_borr`(长期借款), `bond_payable`(应付债券), `total_ncl`(非流动负债合计)
  - 股东权益: `treasury_share`(库存股), `minority_int`(少数股东权益), `total_hldr_eqy_exc_min_int`(股东权益合计)

**利润表数据说明**:
- 日期格式使用报告期日期，如 `2024-03-31` 表示 2024Q1，`2024-06-30` 表示 2024Q2
- 利润表数据为季度数据，通常在财报发布后更新
- 支持按股票代码查询，如 `get_income('600519.SH', '2020-03-31', '2024-12-31')`
- 返回字段包含100+个利润表指标，包括:
  - 每股收益: `basic_eps`(基本每股收益), `diluted_eps`(稀释每股收益)
  - 收入类: `total_revenue`(营业总收入), `revenue`(营业收入), `int_income`(利息收入), `comm_income`(手续费收入)
  - 成本费用: `total_cogs`(营业成本), `oper_cost`(营业成本), `int_exp`(利息支出), `fin_exp`(财务费用), `rd_exp`(研发费用)
  - 利润: `operate_profit`(营业利润), `total_profit`(利润总额), `income_tax`(所得税费用), `n_income`(净利润), `ebit`(息税前利润), `ebitda`(息税折旧摊销前利润)

**频率选项** (用于 `get_minute`):
- `minute_1` - 1分钟线
- `minute_5` - 5分钟线
- `minute_15` - 15分钟线
- `minute_30` - 30分钟线
- `minute_60` - 60分钟线

## CLI 使用示例

### 数据更新命令

```bash
# 使用 --dataset 参数更新数据（推荐）
fdh-cli update --dataset daily              # 更新日线数据
fdh-cli update --dataset daily_basic        # 更新每日基本面
fdh-cli update --dataset minute_1           # 更新1分钟数据
fdh-cli update --dataset minute_5           # 更新5分钟数据

# 更新指定股票
fdh-cli update --dataset daily --symbols 600519.SH,000858.SZ

# 强制全量更新
fdh-cli update --dataset daily --force

# 指定日期范围
fdh-cli update --dataset daily --start-date 2024-01-01 --end-date 2024-12-31

# 批量更新指定交易日所有股票
fdh-cli update --dataset daily --trade-date 2024-11-27

# 使用 --frequency 参数（向后兼容）
fdh-cli update --dataset basic            # 股票基本信息
fdh-cli update --dataset adj_factor       # 复权因子

# 更新GDP宏观经济数据
fdh-cli update --dataset gdp                # 智能增量更新
fdh-cli update --dataset gdp --force        # 强制全量更新
fdh-cli update --dataset gdp --start-date 2020-03-31 --end-date 2024-12-31  # 指定日期范围

# 更新PPI宏观经济数据
fdh-cli update --dataset ppi                # 智能增量更新
fdh-cli update --dataset ppi --force        # 强制全量更新
fdh-cli update --dataset ppi --start-date 2020-01-31 --end-date 2024-12-31  # 指定日期范围

# 更新货币供应量数据
fdh-cli update --dataset m                  # 智能增量更新
fdh-cli update --dataset m --force          # 强制全量更新
fdh-cli update --dataset m --start-date 2020-01-31 --end-date 2024-12-31  # 指定日期范围

# 更新PMI采购经理人指数数据
fdh-cli update --dataset pmi                # 智能增量更新
fdh-cli update --dataset pmi --force        # 强制全量更新
fdh-cli update --dataset pmi --start-date 2020-01-31 --end-date 2024-12-31  # 指定日期范围

# 更新大盘指数每日指标数据
fdh-cli update --dataset index_dailybasic   # 智能增量更新
fdh-cli update --dataset index_dailybasic --force  # 强制全量更新
fdh-cli update --dataset index_dailybasic --start-date 2024-01-01 --end-date 2024-12-31  # 指定日期范围
fdh-cli update --dataset index_dailybasic --symbols 000001.SH  # 指定指数代码

# 更新财务指标数据
fdh-cli update --dataset fina_indicator                    # 智能增量更新（需要指定股票）
fdh-cli update --dataset fina_indicator --force            # 强制全量更新
fdh-cli update --dataset fina_indicator --symbols 600519.SH,000858.SZ  # 指定股票代码
fdh-cli update --dataset fina_indicator --symbols 600519.SH --start-date 2020-03-31 --end-date 2024-12-31  # 指定日期范围

# 更新现金流量表数据
fdh-cli update --dataset cashflow                          # 智能增量更新（需要指定股票）
fdh-cli update --dataset cashflow --force                  # 强制全量更新
fdh-cli update --dataset cashflow --symbols 600519.SH,000858.SZ  # 指定股票代码
fdh-cli update --dataset cashflow --symbols 600519.SH --start-date 2020-03-31 --end-date 2024-12-31  # 指定日期范围

# 更新资产负债表数据
fdh-cli update --dataset balancesheet                      # 智能增量更新（需要指定股票）
fdh-cli update --dataset balancesheet --force              # 强制全量更新
fdh-cli update --dataset balancesheet --symbols 600519.SH,000858.SZ  # 指定股票代码
fdh-cli update --dataset balancesheet --symbols 600519.SH --start-date 2020-03-31 --end-date 2024-12-31  # 指定日期范围

# 更新利润表数据
fdh-cli update --dataset income                            # 智能增量更新（需要指定股票）
fdh-cli update --dataset income --force                    # 强制全量更新
fdh-cli update --dataset income --symbols 600519.SH,000858.SZ  # 指定股票代码
fdh-cli update --dataset income --symbols 600519.SH --start-date 2020-03-31 --end-date 2024-12-31  # 指定日期范围
```

### 日志输出控制

```bash
# 默认模式（只显示 ERROR 级别日志）
fdh-cli update --dataset daily

# 详细模式（显示 INFO 级别日志，包括路由决策、进度等）
fdh-cli update --dataset daily --verbose
fdh-cli update --dataset daily -v

# 安静模式（只显示关键信息，减少日志输出）
fdh-cli update --dataset daily --quiet
fdh-cli update --dataset daily -q

# 组合使用：详细输出但不显示冗余信息
fdh-cli update --dataset daily --verbose --quiet
```

**日志级别说明**:
- 默认: ERROR（只显示错误信息）
- `--verbose` / `-v`: INFO（显示详细进度、路由决策、统计信息）
- `--quiet` / `-q`: 只显示关键结果和错误

### 高周期聚合管理

```bash
# 手动刷新聚合视图
fdh-cli refresh-aggregates --table symbol_weekly --start 2024-01-01 --end 2024-12-31
fdh-cli refresh-aggregates --table symbol_monthly
fdh-cli refresh-aggregates --table daily_basic_weekly

# 查看聚合状态
fdh-cli status --aggregates
```

### 系统状态查看

```bash
# 查看数据库状态
fdh-cli status

# 详细信息
fdh-cli status --verbose
```

### 配置管理

```bash
# 查看当前配置
fdh-cli config show

# 测试配置
fdh-cli config test
```

### 数据库清理

```bash
# 完全清理数据库（删除所有表、视图、函数、连续聚合）- 需确认
fdh-cli cleanup --mode all

# 完全清理并跳过确认
fdh-cli cleanup --mode all --yes

# 只清空数据，保留表结构
fdh-cli cleanup --mode data_only

# 只删除连续聚合视图
fdh-cli cleanup --mode aggregates

# 显示详细信息
fdh-cli cleanup --mode all --verbose
```

**注意**: 数据库清理操作不可逆，请谨慎使用！
