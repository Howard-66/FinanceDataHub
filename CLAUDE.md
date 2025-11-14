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

**阶段**: 设计与规划中
- ✅ 详细的架构设计文档（README.md）
- ❌ 尚未开始编码实现
- 📋 下一阶段：Phase 1 - 环境搭建与配置管理

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
```

## 实施计划

### Phase 1: 环境与配置 (当前阶段)
- [ ] 编写 `docker-compose.yml` 部署 PG+TimescaleDB 和 Redis
- [ ] 实现基于 Pydantic 的配置模块 (`config.py`)
- [ ] 搭建 `fdh-cli` 基本框架

### Phase 2: 核心批处理流程
- [ ] 实现 `TushareProvider`（直接API调用）
- [ ] 实现 `XTQuantProvider`（作为 `xtquant_helper` 的HTTP API客户端）
- [ ] 定义并实现 `sources.yml` 配置加载及智能数据源路由逻辑
- [ ] 实现 `fdh-cli update` 命令
- [ ] 实现 `fdh-cli etl` 命令

### Phase 3: 数据访问与查询
- [ ] 封装 `FinanceDataHub` SDK 类
- [ ] 实现基于 PG 和 DuckDB 的基础查询接口
- [ ] 在 SDK 中加入"智能路由"逻辑和异步接口

### Phase 4: 流式处理与高级特性
- [ ] 在 `xtquant_helper` 服务中增加 WebSocket 接口
- [ ] 实现流式 `Provider` 连接 WebSocket
- [ ] 实现数据到 Redis Pub/Sub 的发布
- [ ] 编写 `Archiver` 服务持久化流式数据
- [ ] 对接 Qlib/FinRL 数据格式的导出功能

### Phase 5: 测试与部署
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

```python
from finance_data_hub import FinanceDataHub

# 初始化（需要先实现）
fdh = FinanceDataHub(settings, backend='auto')

# 同步API
daily_data = fdh.get_daily(['600519.SH', '000858.SZ'], '2024-01-01', '2024-12-31')

# 异步API
async def get_data():
    return await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')

# 触发数据更新
fdh.trigger_update(mode='incremental', source='tushare')
```

## CLI 使用示例

（实施后的命令）

```bash
# 更新数据
fdh-cli update --asset-class stock --frequency daily

# ETL 数据
fdh-cli etl --from-date 2024-01-01

# 检查状态
fdh-cli status

# 强制全量刷新
fdh-cli update --mode=full
```
