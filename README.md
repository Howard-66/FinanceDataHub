# FinanceDataHub需求

构建一个高效、可扩展、易于维护的金融数据服务中心（FinanceDataHub），为行情显示、量化分析、投资研究和策略回测提供统一、可靠、高性能的数据基础。
## 核心原则：
- 开放与扩展性 (Openness & Extensibility): 架构设计必须是开放的，能够轻松接入新的数据源和存储引擎，避免供应商锁定。
- 服务解耦 (Decoupling): 严格分离数据获取、数据存储和数据访问三个核心环节，确保数据服务本身与上层分析应用解耦。
- 性能优先 (Performance First): 在数据存储和访问层面，优先采用列式存储和内存计算技术，以满足金融数据高频读写的性能要求。
- 开发者友好 (Developer-Friendly): 提供简洁、直观的 API，让开发者可以专注于数据的使用而非数据的获取与管理。
- 数据一致性 (Data Consistency): 提供明确的数据更新机制（增量、全量），确保本地数据副本的准确性和时效性。
- 可观测行: 完整的日志记录和性能监控。
- 项目环境管理：虚拟环境和包使用uv进行管理
## 需求范围：
- 统一的数据接口: 提供一个统一的接口，用于获取不同来源、不同市场的金融数据。
- 多数据源接入: 设计可插拔的数据源模块，初期支持 Tushare和XTQuant 。
- 本地持久化存储: 设计可插拔的存储模块，初期支持 PostgreSQL+TimeScale，后期出于新能需要以及截面数据分析，可考虑使用DuckDB+ Parquet分区存储，便于为qlib、FinRL这样的平台，提供可方便访问和转换的数据。
- 数据更新管理: 支持离线、增量更新、全量强制更新三种模式。支持定时数据下载和预加工。后期需要支持行情推送（用于K线显示、行情监控预警）
- 多市场支持: 优先支持中国A股和港股，架构上需考虑对期货、期权、美股、加密货币等的兼容扩展。
## 不在本项目范围内的需求
- 投资策略与分析功能: 本项目不包含任何具体的交易信号生成、策略回测框架或数据分析工具。它只提供数据。
- 策略专属 Alpha 研究: 本项目会提供可复用的标准化预处理能力，例如复权、技术指标、基本面质量/估值、中国宏观周期与行业快照；但不负责策略特定的因子挖掘、组合优化或交易信号设计。
- 用户界面 (UI): 本项目核心是一个供其他程序调用的库或服务，不包含任何图形用户界面。

---

# FinanceDataHub 设计方案 (v2.0)

该方案采纳了社区建议进行了优化，旨在构建一个**健壮、可扩展、易于维护且具备前瞻性**的金融数据服务中心。方案采用**分层架构**、**插件化设计**和**事件驱动**的混合模式。

## 1. 核心原则

- **开放与扩展性**: 轻松接入新数据源、存储引擎和数据类别。
- **服务解耦**: 严格分离数据获取、存储、访问和流处理。
- **性能优先**: 混合使用OLTP数据库、列式存储和内存计算。
- **开发者友好**: 提供简洁、直观、支持同步/异步的API。
- **数据一致性与可靠性**: 明确的数据更新与容错机制。
- **可观测与可管理性**: 结构化日志、监控以及CLI管理工具。

---

## 2. 总体架构图 (High-Level Architecture)

系统自下而上分为：**数据源层**、**核心服务层**、**存储层**、**数据访问层**，并新增了**流处理总线**。

```mermaid
graph TD
    subgraph "应用层 (User Application)"
        A1[量化回测引擎 (Qlib)]
        A2[AI训练 (FinRL)]
        A3[数据分析 (Jupyter)]
        A4[实时行情看板 (Websocket)]
    end

    subgraph "数据访问层 (Data Access SDK)"
        API[FDH Client SDK (Sync/Async)]
        Router{智能路由}
    end

    subgraph "存储层 (Storage Layer)"
        S1[(Hot/Warm: PostgreSQL + TimescaleDB)]
        S2[(Cold/Analytics: Parquet Files)]
        Engine[DuckDB 查询引擎]
    end

    subgraph "核心服务与调度 (Core Service & Scheduling)"
        CLI[fdh-cli (Typer/Click)]
        Scheduler[调度器 (APScheduler/Crontab)]
        SourceRouter{智能数据源路由}
        subgraph "CLI Commands"
            direction LR
            CLI_Update[update]
            CLI_ETL[etl]
            CLI_Status[status]
        end
        Log[日志与监控]
        Config[配置模块 (Pydantic)]
    end

    subgraph "数据源层 (Provider Layer)"
        subgraph "批处理适配器 (Batch Adapters)"
            P_Batch1[Tushare]
            P_Batch2[...]
        end
        subgraph "流处理适配器 (Stream Adapters)"
            P_Stream1[XTQuant]
            P_Stream2[...]
        end
    end
    
    subgraph "新增：流处理总线 (New: Streaming Bus)"
        MQ([Redis Pub/Sub])
    end

    %% 关系连线
    %% 应用层 -> SDK
    A1 & A2 & A3 --> API
    A4 -- "订阅实时数据" --> MQ

    %% SDK -> 存储
    API --> Router
    Router --"实时/事务查询"--> S1
    Router --"大规模批量/复杂分析"--> Engine
    Engine --> S2

    %% 核心服务
    Scheduler -- "调用" --> CLI
    CLI -- "使用" --> Config & Log
    CLI_Update -- "请求数据" --> SourceRouter
    SourceRouter -- "选择并调用" --> P_Batch1 & P_Batch2
    CLI_ETL -- "读取" --> S1
    CLI_ETL -- "写入" --> S2

    %% 批处理数据流
    P_Batch1 & P_Batch2 -- "标准化" --> S1

    %% 流处理数据流
    P_Stream1 & P_Stream2 -- "发布 Tick/Bar" --> MQ
    subgraph "流数据消费者"
        direction LR
        Archiver[归档服务]
    end
    Archiver -- "订阅并批量写入" --> S1
    
    %% 依赖关系
    style CLI fill:#cde4ff,stroke:#333,stroke-width:2px
    style MQ fill:#ffc,stroke:#333,stroke-width:2px
```

---

## 3. 核心模块设计

#### 3.1. 新增：配置管理 (Configuration Management)

- **目标**：将配置（如API密钥、数据库连接）与代码分离，实现安全、灵活的环境配置。
- **设计**：
    - 使用 `.env` 文件存储敏感信息。
    - 使用 `Pydantic` 的 `BaseSettings` 来加载、校验和管理应用主配置。
    - **新增数据源能力配置**：创建一个独立的配置文件（如 `sources.yml`），用于定义每个数据源的能力范围和优先级。这使得数据源的选择逻辑完全由配置驱动，而非硬编码。
    - 配置对象通过**依赖注入**的方式传入需要它的模块中。
- **数据源配置示例 (`sources.yml`)**:
  ```yaml
  providers:
    tushare:
      token: "your_tushare_token"
    xtquant:
      api_url: "http://localhost:8100" # xtquant_helper 服务地址

  # 定义不同数据类型的获取策略
  routing_strategy:
    stock:
      daily: [tushare, xtquant]  # 日线数据优先用tushare，失败则用xtquant
      minute_1: [xtquant]         # 1分钟线只能用xtquant
      tick: [xtquant]             # Tick数据只能用xtquant
    indicator:
      daily: [tushare]          # 每日指标用tushare
  ```

#### 3.2. 数据源层 (Provider Layer) - "适配器模式"

- **目标**：屏蔽不同数据源的接口差异，输出统一格式的数据。
- **接口定义**：`BaseDataProvider` 定义了 `fetch_daily`, `fetch_minutes` 等接口。
- **标准化**：所有 Provider 返回的数据必须转换为统一的 Pandas DataFrame 结构，并包含标准化的元数据。
    - **统一列名**: `time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adj_factor`。
    - **统一Symbol格式**: `<code>.<exchange>`，例如 `600519.SH`。

- #### **`XTQuantProvider` 特殊实现**
    - **架构**：由于 `XTQuant` 只能在 Windows 环境运行，`XTQuantProvider` 将作为一个 **API 客户端** 来实现。它不直接依赖 `xtquant` SDK。
    - **通信**：它通过 HTTP 请求与一个独立部署在 Windows 服务器上的 `xtquant_helper` 微服务进行交互。该服务的地址由配置文件中的 `api_url` 指定。
    - **实现细节**：Provider 内部将使用 `httpx` 或 `requests` 库。其 `fetch_` 方法会：
        1.  从配置中读取 `api_url`。
        2.  根据方法参数构建一个 JSON 请求体。
        3.  向 `xtquant_helper` 服务的相应端点 (如 `/get_market_data`) 发送 POST 请求。
        4.  处理 HTTP 响应和潜在错误。
        5.  将返回的 JSON 数据转换为标准化的 Pandas DataFrame。

#### 3.3. 存储层 (Storage Layer) - "冷热分离"

- **主存储 (Source of Truth)**: **PostgreSQL + TimescaleDB**
    - **角色**：负责数据的首次写入、修正、增量更新。作为所有数据的真实来源。
    - **数据模型**:
        - **`asset_basic` (资产元数据表)**: 存储代码、名称、上市/退市日期、资产类别 (`Stock`, `Future`)等。
        - **`symbol_daily` (日线行情表)**: 包含 `time`, `symbol`, `o,h,l,c,v,a` 以及 `adj_factor` (复权因子)。
- **分析存储 (Analytics Store)**: **Parquet + DuckDB**
    - **角色**：为AI训练和复杂分析优化的只读副本。
    - **分区策略**:
        - `data/{asset_class}/by_symbol/{symbol}.parquet` (按资产、按代码分区)
        - `data/{asset_class}/by_date/year={YYYY}/month={MM}.parquet` (按资产、按日期分区)

#### 3.4. 核心服务层 (Core Service) - "CLI驱动与智能数据源路由"

- **目标**：提供一组原子化的管理工具，并实现数据源的智能选择。
- **设计**：基于 `Typer` 或 `Click` 实现 `fdh-cli` 命令行工具。
    - **`fdh-cli update`**: 负责从数据源拉取数据。其核心是**智能数据源路由器 (Smart Source Router)**。
        - **工作流程**：
            1.  命令接收到更新请求，例如 `fdh-cli update --asset-class stock --frequency daily`。
            2.  `SourceRouter` 加载 `sources.yml` 中的 `routing_strategy` 配置。
            3.  根据请求的资产类别 (`stock`) 和频率 (`daily`)，查找对应的服务商列表 `[tushare, xtquant]`。
            4.  按顺序尝试：首先调用 `TushareProvider`。如果成功，任务完成。
            5.  如果 `TushareProvider` 失败（如API限流、网络错误、或当日无数据），则自动调用列表中的下一个服务商 `XTQuantProvider` 作为**故障转移 (Fallback)**。
    - **`fdh-cli etl`**: 负责将主数据库的数据同步到分析存储（Parquet）。
    - **`fdh-cli status`**: 负责监控数据完整性和服务状态。
- **容错机制**: 除了数据源的故障转移，单次API请求也应集成 `tenacity` 等重试库，确保在网络波动时任务的稳定性。

#### 3.5. 新增：流式处理总线 (Streaming Bus)

- **目标**：为实时行情推送、监控预警等场景提供低延迟的数据通道。
- **设计**：
    - 使用 `Redis Pub/Sub` 作为轻量级消息中间件。
    - 实时数据源 (如 `XTQuantProvider`) 将获取到的 `tick` 或 `bar` 数据发布到指定主题 (e.g., `streaming.ticks.SH.600519`)。
    - **对 `XTQuant` 的特殊要求**：为了支持流式数据，`xtquant_helper` 微服务**必须额外提供一个 WebSocket 接口**。`FinanceDataHub` 中的流式 `XTQuantProvider` 将连接到此 WebSocket 端点来接收实时行情，然后再将其发布到 Redis。
    - 下游应用（看板、预警服务）按需订阅。
    - 一个独立的 `Archiver` 服务订阅所有行情主题，将流式数据批量持久化到 TimescaleDB。

#### 3.6. 数据访问层 (Data Access SDK) - "智能外观模式"

- **目标**：为最终用户提供一个极其简单、高性能的Python SDK。
- **设计**：
    - **智能路由**: SDK内部根据查询的**数据量、时间范围、实时性要求**，自动选择从 PG 或 DuckDB 查询。
    - **同步/异步接口**: 同时提供 `get_daily()` 和 `get_daily_async()` 方法，满足不同应用场景的需求。
    - **数据封装**: 返回统一的、包含元数据的 Pandas DataFrame。

---

## 4. 接口定义示例 (Python SDK v2.0)

```python
import asyncio
import pandas as pd
from .config import Settings # 引入Pydantic配置

class FinanceDataHub:
    def __init__(self, settings: Settings, backend='auto'):
        """
        初始化时直接传入配置对象，实现依赖注入。
        :param settings: 配置实例
        :param backend: 'auto' (智能路由), 'pg' (强制PG), 'duck' (强制DuckDB)
        """
        self.settings = settings
        self.backend = backend
        # ... 根据 settings 初始化 pg_engine 和 duck_conn ...

    def get_daily(
        self, symbols: list, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        获取日线数据 (同步版本)
        """
        # 智能路由逻辑...
        pass

    async def get_daily_async(
        self, symbols: list, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """
        获取日线数据 (异步版本)
        """
        # 异步的智能路由和查询逻辑...
        loop = asyncio.get_running_loop()
        # 实际实现会使用异步数据库驱动
        df = await loop.run_in_executor(
            None, self.get_daily, symbols, start_date, end_date
        )
        return df

    def trigger_update(self, mode='incremental', source='all'):
        """
        触发数据更新任务。
        内部实现将调用 fdh-cli 子进程，解耦执行逻辑。
        """
        import subprocess
        command = [
            "fdh-cli", "update", 
            f"--mode={mode}", 
            f"--source={source}"
        ]
        subprocess.run(command, check=True)
```

---

## 5. 技术栈选型 (v2.0)

|**模块**|**推荐技术**|**理由**|
|---|---|---|
|**开发语言**|Python 3.10+|金融量化标准语言，生态丰富。|
|**配置管理**|Pydantic, .env file|类型安全，与环境变量无缝集成。|
|**核心服务CLI**|Typer / Click|构建健壮、易于测试的命令行工具。|
|**主数据库**|PostgreSQL 16 + TimescaleDB|处理时序数据的最佳OLTP/OLAP混合体。|
|**分析引擎**|DuckDB|进程内SQL引擎，分析Parquet极快，无服务部署。|
|**文件格式**|Parquet (Zstd压缩)|列式存储黄金标准，高压缩比，生态通用。|
|**调度框架**|APScheduler / Crontab|轻量级，易于集成，调用CLI命令。|
|**流处理总线**|Redis Pub/Sub|轻量、快速，满足大部分实时场景，易于部署。|
|**ORM/连接**|SQLAlchemy (Core), asyncpg|Python数据库操作标准，支持异步。|
|**日志**|Loguru|优雅、简单的结构化日志库。|
|**部署**|Docker Compose|一键拉起PG、Redis和Python服务环境。|

---

## 6. 下一步实施计划 (Revised)

1.  **Phase 1 (环境与配置)**:
    - 编写 `docker-compose.yml` 部署 PG+TimescaleDB 和 Redis。
    - 实现基于 Pydantic 的配置模块 (`config.py`)。
    - 搭建 `fdh-cli` 的基本框架。

2.  **Phase 2 (核心批处理流程)**:
    - 实现 `TushareProvider` (直接API调用) 和 `XTQuantProvider` (作为 `xtquant_helper` 的HTTP API客户端)，并定义标准数据表结构。
    - **定义并实现 `sources.yml` 配置加载及智能数据源路由逻辑**。
    - 实现 `fdh-cli update` 命令，集成Source Router，跑通“数据源 -> TimescaleDB”的完整流程。
    - 实现 `fdh-cli etl` 命令，跑通“TimescaleDB -> Parquet”的同步流程。

3.  **Phase 3 (数据访问与查询)**:
    - 封装 `FinanceDataHub` SDK 类。
    - 【暂不实现】实现基于 PG 和 DuckDB 的基础查询接口。
    - 在 SDK 中加入“智能路由”逻辑和异步接口。

4.  **Phase 4 (流式处理与高级特性)**:
    - **在 `xtquant_helper` 服务中增加 WebSocket 接口** 用于实时行情订阅。
    - 在 `FinanceDataHub` 中实现连接到该 WebSocket 的流式 `Provider`。
    - 实现数据到 Redis Pub/Sub 的发布。
    - 编写一个简单的 `Archiver` 服务，将流式数据持久化。
    - 对接 Qlib/FinRL 数据格式的导出功能。

5.  **Phase 5 (测试与部署)**:
    - 为 CLI 命令和数据 Provider 编写单元测试 (使用 `pytest` 和 `mock`)。
    - 完善 Dockerfile 和部署脚本。

---

## 📦 Phase 1 & 2: 全部完成 ✅

Phase 1 和 Phase 2 已全部完成！系统已具备生产就绪能力。

### ✅ 已完成功能

#### Phase 1 - 环境与配置
- ✅ **Docker Compose 服务**: PostgreSQL + TimescaleDB, Redis 7.x
- ✅ **配置管理**: 基于 Pydantic 类型安全配置 + .env
- ✅ **fdh-cli 工具**: 4个核心命令（update, etl, status, config）
- ✅ **项目结构**: 标准 Python 包 + uv 依赖管理 + 测试套件

#### Phase 2 - 核心批处理流程
- ✅ **数据提供者**: TushareProvider (直连API) + XTQuantProvider (HTTP客户端)
- ✅ **智能路由**: sources.yml 配置驱动 + 断路器模式 + 自动故障转移
- ✅ **数据库层**: 5张核心表 (asset_basic, daily_basic, symbol_daily, symbol_minute, adj_factor)
- ✅ **智能下载模式**: 自动检测数据库状态，新symbol全量，有symbol增量
- ✅ **强制更新模式**: `--force` 参数忽略数据库状态，强制覆盖
- ✅ **交易日批量更新**: `--trade-date` 参数批量获取指定交易日所有股票
- ✅ **复权因子**: 完整的前/后复权因子管理（增量更新）

#### Phase 2.5 - 高周期数据聚合 ✅
- ✅ **TimescaleDB 连续聚合**: 自动维护周线、月线数据聚合
- ✅ **6 个聚合视图**:
  - `symbol_weekly` - 周线 OHLCV 数据（带复权处理）
  - `symbol_monthly` - 月线 OHLCV 数据（带复权处理）
  - `daily_basic_weekly` - 周线聚合基础指标
  - `daily_basic_monthly` - 月线聚合基础指标
  - `adj_factor_weekly` - 周线复权因子聚合
  - `adj_factor_monthly` - 月线复权因子聚合
- ✅ **智能刷新策略**: 1小时自动刷新，实时物化
- ✅ **SDK 支持**: `FinanceDataHub.get_weekly()`, `get_monthly()` 等方法
- ✅ **CLI 管理**: `fdh-cli refresh-aggregates`, `fdh-cli status --aggregates`
- ✅ **数据验证**: 提供验证脚本确保聚合准确性（< 0.01% 误差）

#### 核心特性
- ✅ **多数据源整合**: 统一 Tushare 和 XTQuant 接口
- ✅ **智能故障转移**: 断路器 + 自动切换
- ✅ **企业级性能**: 连接池 + 批量写入 + API限频控制
- ✅ **开发者友好**: Rich 美观CLI + 详细日志 + 进度条

### 🚀 快速开始

```bash
# 1. 启动服务
docker-compose up -d

# 2. 安装依赖
uv sync

# 3. 配置环境变量
cp .env.example .env
# 编辑 .env，设置 TUSHARE_TOKEN 和数据库连接

# 4. 初始化数据库
psql "$DATABASE_URL" -f sql/init/001_create_extensions.sql
psql "$DATABASE_URL" -f sql/init/002_create_tables.sql
psql "$DATABASE_URL" -f sql/init/003_create_hypertables.sql
psql "$DATABASE_URL" -f sql/init/004_create_adj_factor.sql
psql "$DATABASE_URL" -f sql/init/005_create_functions.sql
psql "$DATABASE_URL" -f sql/init/006_create_continuous_aggregates.sql
psql "$DATABASE_URL" -f sql/init/007_create_preprocess_tables.sql

# 已有数据库升级到当前版本时，执行新增迁移
psql "$DATABASE_URL" -f sql/migrations/025_add_daily_valuation_fill.sql

# 5. 获取数据

```bash
# 控制台输出级别（默认 ERROR 级别，减少刷屏）
fdh-cli update --dataset daily              # 默认安静模式，只显示必要信息
fdh-cli update --dataset daily -v           # 详细模式，显示 INFO 日志
fdh-cli update --dataset daily -q           # 安静模式（默认），日志级别 ERROR

# 港股股票列表（通过 Tushare hk_basic 获取）
fdh-cli update --dataset basic --market HK

# 智能下载模式（默认）- 自动检测数据库状态
fdh-cli update --dataset daily              # 自动增量更新所有股票
fdh-cli update --dataset daily_basic        # 自动增量更新每日指标
fdh-cli update --symbols 600519.SH,000858.SZ # 更新指定股票

# 港股日线 / 分钟线 / 复权因子前置条件：确保 xtquant_helper 可访问
# .env: XTQUANT_API_URL=http://<your-xtquant-helper-host>:8100
fdh-cli update --dataset daily --market HK
fdh-cli update --dataset minute_1 --market HK --symbols 00700.HK
fdh-cli update --dataset adj_factor --market HK

# 同时更新 A 股和港股支持的数据集
fdh-cli update --dataset basic --market ALL
fdh-cli update --dataset daily --market ALL

# 强制更新模式 - 忽略数据库状态
fdh-cli update --dataset daily --force      # 强制全量更新所有股票
fdh-cli update --dataset daily --force --start-date 2024-01-01 # 指定日期范围
fdh-cli update --dataset daily --market HK --force --start-date 2024-01-01 --end-date 2024-12-31

# 期货数据
fdh-cli update --asset-class future --dataset basic
fdh-cli update --asset-class future --dataset mapping --symbols all --start-date 2024-04-01 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset daily --symbols all --start-date 2024-04-01 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset daily --symbols RB2405.SHF --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset minute_1 --symbols rb2405.SF --start-date "2024-04-30 09:30:00" --end-date "2024-04-30 10:00:00"
fdh-cli update --asset-class future --dataset settle --symbols RB2405.SHF --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset index_daily --symbols NHCI.NH --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset spot_basis --symbols RB --trade-date 2024-04-30
fdh-cli update --asset-class future --dataset inventory --symbols RB --start-date 2024-04-30 --end-date 2024-04-30
# 说明: --symbols all 会按 futures.contract_basic 展开全量合约池

# 交易日批量更新 - 批量获取指定交易日所有股票
fdh-cli update --dataset daily --trade-date 2024-11-18
fdh-cli update --dataset daily_basic --trade-date 2024-11-18
fdh-cli update --dataset daily --market HK --trade-date 2024-11-18
fdh-cli update --dataset adj_factor --market HK --trade-date 2024-11-18
# 注意: index_daily 不支持 --trade-date 全指数单日批量模式

# 估值缺失补值预处理（A股）
# 前置: daily_basic + income + balancesheet + fina_indicator 已更新
fdh-cli preprocess run --all --category valuation_fill

# 基于 enriched daily_basic 继续计算估值分位
fdh-cli preprocess run --all --category fundamental

# 首次全量重建
fdh-cli preprocess run --all --category valuation_fill --force

# 查看预处理模块信息和状态
fdh-cli preprocess info
fdh-cli preprocess status

# 向后兼容 - 仍支持 --frequency 参数
fdh-cli update --frequency basic            # 股票基本信息
fdh-cli update --frequency daily            # 日线数据（已废弃，请使用 --dataset）
fdh-cli update --frequency adj_factor       # 复权因子

# 6. 高周期数据聚合（可选）
# 连续聚合会自动创建并每小时刷新一次
# 手动刷新指定聚合
fdh-cli refresh-aggregates --table symbol_weekly --start-date 2024-01-01 --end-date 2024-12-31
# 查看聚合状态
fdh-cli status --aggregates
# 验证聚合准确性
python scripts/validate_aggregates.py --symbol 600519.SH --year 2024

# 7. 查看状态
fdh-cli status --verbose

# 8. Python SDK 使用示例
python3 << 'EOF'
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

settings = get_settings()
fdh = FinanceDataHub(settings)

# 获取周线数据
weekly = fdh.get_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"周线数据: {len(weekly)} 条")

# 获取月线数据
monthly = fdh.get_monthly(['000858.SZ'], '2020-01-01', '2024-12-31')
print(f"月线数据: {len(monthly)} 条")

# 获取周线基础指标
weekly_metrics = fdh.get_daily_basic_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"周线指标: {len(weekly_metrics)} 条")

# 获取周线复权因子
weekly_adj = fdh.get_adj_factor_weekly(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"周线复权因子: {len(weekly_adj)} 条")

# 获取逐字段补值后的每日估值
filled_basic = fdh.get_daily_basic(
    ['600519.SH'],
    '2024-01-01',
    '2024-12-31',
    filled=True,
)
print(
    filled_basic[
        ['symbol', 'time', 'pe_ttm', 'pb', 'ps_ttm', 'peg', 'pe_ttm_source']
    ].tail()
)

# 获取月线复权因子
monthly_adj = fdh.get_adj_factor_monthly(['600519.SH'], '2020-01-01', '2024-12-31')
print(f"月线复权因子: {len(monthly_adj)} 条")

# 获取期货数据
contracts = fdh.get_futures_contracts(product_codes=['RB'])
daily = fdh.get_futures_daily(symbols=['RB2405.SHF'], start_date='2024-04-30', end_date='2024-04-30')
basis = fdh.get_futures_spot_basis(product_codes=['RB'], start_date='2024-04-30', end_date='2024-04-30')
print(f"期货合约: {len(contracts)} 条")
print(f"期货日线: {len(daily)} 条")
print(f"期货基差: {len(basis)} 条")
EOF
```

### 测试结果

- ✅ **单元测试**: 42/42 通过 (100%)
- ✅ **代码覆盖率**: >80%
- ✅ **类型检查**: mypy 无错误
- ✅ **代码质量**: black + isort + flake8 通过

### 性能指标

- **首次全量更新**: 30分钟 (5000只股票)
- **增量更新**: 10秒 (仅更新股票)
- **API调用优化**: 增量更新节省99.8%调用次数
- **内存占用**: ~50MB (基线)

### 支持的数据集更新

| 命令 | 功能 | 示例 |
|------|------|------|
| `basic` | 股票基本信息 | `fdh-cli update --dataset basic` |
| `daily` | 日线行情 | `fdh-cli update --dataset daily --symbols 600519.SH` |
| `minute_1` | 1分钟数据 | `fdh-cli update --dataset minute_1 --symbols 600519.SH` |
| `minute_5` | 5分钟数据 | `fdh-cli update --dataset minute_5 --symbols 600519.SH` |
| `adj_factor` | 复权因子 | `fdh-cli update --dataset adj_factor` |
| `index_daily` | 指数日线行情 | `fdh-cli update --dataset index_daily --symbols all` |

### A 股估值缺失补值

`daily_basic` 保持为 Tushare 原始镜像，补值结果写入独立表
`processed_daily_valuation_fill`，并通过 `v_daily_basic_enriched` 逐字段合并。

典型流程：

```bash
# 1. 更新原始估值和财报数据
fdh-cli update --dataset daily_basic
fdh-cli update --dataset income
fdh-cli update --dataset balancesheet
fdh-cli update --dataset fina_indicator

# 2. 生成派生估值层
fdh-cli preprocess run --all --category valuation_fill

# 3. 在 enriched 输入上重算估值分位
fdh-cli preprocess run --all --category fundamental
```

口径说明：
- `pe_ttm`、`pb`、`ps_ttm`、`peg` 支持财报派生补值
- `pe`、`ps` 使用最新已公告年报口径补值，并通过 `*_source` 标记来源
- 当 TTM 数据窗口不完整、无法可靠计算 `pe_ttm` / `ps_ttm` 时，会回退使用年报口径的 `pe` / `ps`，并通过 `*_source` 与 `quality_flags` 标记为 fallback
- `dv_ratio`、`dv_ttm` 当前不做财报近似补值，仍以 raw 为准
- SDK 默认返回 raw；传 `filled=True` 才会读取 `v_daily_basic_enriched`

### 港股 CLI 指南

港股 v1 当前支持：
- `basic`：港股股票列表，通过 Tushare `hk_basic` 获取并映射到统一主数据字段
- `daily`：港股日线，保存原始未复权 K 线
- `minute_1` / `minute_5` / `minute_60`：港股分钟线（XtQuant 原生支持 `1m` / `5m` / `1h`）
- `adj_factor`：港股复权因子，基于 Akshare `stock_hk_daily(..., adjust='qfq-factor')` 归一化后生成

港股 v1 当前不支持：
- `daily_basic`
- 港股财务、估值类数据集

推荐顺序：

```bash
# 1. 刷新港股股票池
fdh-cli update --dataset basic --market HK

# 2. 更新港股日线
fdh-cli update --dataset daily --market HK

# 3. 更新港股复权因子
fdh-cli update --dataset adj_factor --market HK

# 4. 按需更新分钟线
fdh-cli update --dataset minute_1 --market HK --symbols 00700.HK,00005.HK
```

常用命令：

```bash
# 指定港股代码
fdh-cli update --dataset daily --market HK --symbols 00700.HK,00941.HK

# 指定历史区间
fdh-cli update --dataset daily --market HK --force \
  --start-date 2024-01-01 --end-date 2024-12-31

# 指定交易日
fdh-cli update --dataset daily --market HK --trade-date 2024-11-18

# 5分钟线
fdh-cli update --dataset minute_5 --market HK --symbols 00700.HK
```

注意：
- 港股 `daily` / `minute_*` 链路依赖 `XTQUANT_API_URL` 指向可用的 `xtquant_helper`；`basic` 走 Tushare `hk_basic`；`adj_factor` 走 Akshare，但仍依赖本地 `daily` 数据作为交易日骨架
- `--market ALL` 只对当前已支持多市场的数据集有意义，例如 `basic`、`daily`、`minute_*`、`adj_factor`
- SDK 中 `get_daily_adjusted()` 会基于原始日线和 `adj_factor` 计算港股前复权、后复权

### 港股技术指标预处理

港股日线和 `adj_factor` 更新完成后，可以直接通过 `fdh-cli preprocess` 计算技术指标。

```bash
# 处理全部港股技术指标（日线 + 周线 + 月线）
fdh-cli preprocess run --all --category technical --market HK

# 处理指定港股
fdh-cli preprocess run --category technical --market HK \
  --symbols 00700.HK,00005.HK

# 指定频率与复权类型
fdh-cli preprocess run --all --category technical --market HK \
  --freq daily,weekly,monthly --adjust qfq

# 强制全量重算
fdh-cli preprocess run --all --category technical --market HK --force
```

说明：
- `preprocess run` 默认 `--market CN`，保持既有 A 股行为不变
- 对港股使用 `--all --market HK` 时，会自动按港股股票池筛选，不需要手动传 `--symbols`
- 当前非 `CN` 市场只支持 `technical`；`fundamental`、`quarterly_fundamental`、`industry_valuation`、`all` 仍然仅适用于 A 股

### 调度配置中的 `market`

如果通过 `schedules.yml` 驱动 `fdh-cli`，建议所有股票类任务都显式声明 `market`：

- 现有 A 股下载 / 预处理任务应写成 `market: CN`
- 港股应使用独立任务，例如 `hk_basic_update`、`hk_daily_update`、`hk_adj_factor_update`、`hk_technical_preprocess`

示例：

```yaml
daily_update:
  type: download
  dataset: daily
  params:
    market: CN

hk_daily_update:
  type: download
  dataset: daily
  params:
    market: HK

technical_preprocess:
  type: preprocess
  category: technical
  params:
    all: true
    market: CN

hk_technical_preprocess:
  type: preprocess
  category: technical
  params:
    all: true
    market: HK
```

### 输出控制参数

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-v` / `--verbose` | 显示详细输出 (INFO级别日志) | False |
| `-q` / `--quiet` | 安静模式 (ERROR级别日志) | False |

```bash
# 输出控制示例
fdh-cli update --dataset daily              # 默认 ERROR 级别
fdh-cli update --dataset daily -v           # INFO 级别，显示详细日志
fdh-cli update --dataset daily -q           # ERROR 级别，安静模式
```

### 项目结构

```
finance_data_hub/
├── cli/                    # CLI 工具
├── config.py              # 配置管理
├── providers/             # 数据提供者
│   ├── base.py            # Provider基类 (420行)
│   ├── tushare.py         # Tushare集成 (540行)
│   ├── xtquant.py         # XTQuant集成 (380行)
│   └── registry.py        # 注册机制
├── router/                # 智能路由
│   └── smart_router.py    # 520行，支持断路器+故障转移
├── database/              # 数据库操作
│   ├── manager.py         # 连接池 (160行)
│   └── operations.py      # 批量操作 (320行)
├── update/                # 数据更新器
│   └── updater.py         # 集成所有组件 (280行)
└── utils/                 # 工具函数

sql/init/
├── 001_create_extensions.sql    # 扩展
├── 002_create_tables.sql        # 5张表
├── 003_create_hypertables.sql   # 超表+策略
├── 004_create_adj_factor.sql    # 复权因子表
└── 005_create_functions.sql     # 存储函数

tests/
├── unit/                  # 单元测试 (42个测试)
└── integration/           # 集成测试

配置文件:
├── .env.example          # 环境变量模板
├── sources.yml.example   # 数据源配置示例
├── pyproject.toml        # 项目配置
├── docker-compose.yml    # Docker 编排
└── uv.lock              # 依赖锁定
```

### 📝 文档索引

**用户文档**:
- [快速开始](./QUICK_START.md) - 完整使用示例、故障排除、Python API和开发指南

**阶段交付文档**:
- [Phase 2 最终交付报告](./docs/FINAL_SUMMARY.md) - Phase 2 完整交付文档，包含Bug修复记录、功能验证清单、代码亮点等详细技术信息
- [Phase 3 完整文档](./docs/features/Phase3_Complete_Documentation.md) - Phase 3 数据访问与查询层完整实施文档，包含API文档、使用指南、性能指标和技术架构
- [A 股日度估值缺失补值](./docs/features/daily_valuation_fill.md) - 补值口径、CLI 工作流、SDK `filled=True` 查询方式

**技术文档**:
- [CLAUDE.md](./CLAUDE.md) - AI开发助手指南和项目规范
- [使用示例](./examples/) - SDK 使用示例代码

### 📦 Phase 3: 数据访问与查询层 - 已完成 ✅

Phase 3 已全部完成！SDK 现在提供了完整的金融数据查询功能。

#### ✅ 已完成功能

**1. DataOperations 查询方法（5个）**
- ✅ `get_symbol_daily()` - 日线 OHLCV 数据查询
- ✅ `get_symbol_minute()` - 分钟级 OHLCV 数据查询（支持1/5/15/30/60分钟）
- ✅ `get_daily_basic()` - 每日基本面指标查询，支持 `filled=True` 读取补值视图
- ✅ `get_adj_factor()` - 复权因子查询
- ✅ `get_asset_basic()` - 股票基本信息查询

**2. SDK 查询接口（10个方法对 = 5对同步/异步）**
- ✅ `get_daily()` / `get_daily_async()` - 日线数据
- ✅ `get_minute()` / `get_minute_async()` - 分钟数据
- ✅ `get_daily_basic()` / `get_daily_basic_async()` - 每日基本面，可选逐字段补值
- ✅ `get_adj_factor()` / `get_adj_factor_async()` - 复权因子
- ✅ `get_basic()` / `get_basic_async()` - 股票基本信息

**3. SmartRouter 智能路由集成**
- ✅ 自动读取 `sources.yml` 配置文件
- ✅ 在所有查询方法中集成数据源选择逻辑
- ✅ 实现路由决策日志记录功能
- ✅ 提供数据新鲜度检查 (`check_data_freshness()`)
- ✅ 优雅降级：配置文件不存在或加载失败时仍可正常使用

**4. 核心特性**
- ✅ **自动初始化**: 无需显式调用 `fdh.initialize()`，自动处理数据库连接
- ✅ **Jupyter 兼容**: 完美支持 Jupyter Notebook 中的 `await` 语法
- ✅ **双接口设计**: 同时支持异步和同步调用
- ✅ **优雅降级**: SmartRouter 配置缺失时自动回退到 PostgreSQL
- ✅ **完整类型注解**: 所有方法都有完整的类型提示

#### 📊 支持的数据类型

| 数据类型 | 异步方法 | 同步方法 | 描述 |
|----------|----------|----------|------|
| 日线数据 | `get_daily_async()` | `get_daily()` | OHLCV + 成交量 + 复权因子 |
| 分钟数据 | `get_minute_async()` | `get_minute()` | 1/5/15/30/60分钟线 |
| 每日基本面 | `get_daily_basic_async()` | `get_daily_basic()` | 估值、财务、流动性指标；支持 `filled=True` |
| 复权因子 | `get_adj_factor_async()` | `get_adj_factor()` | 前复权、后复权因子 |
| 基本信息 | `get_basic_async()` | `get_basic()` | 股票基本信息（非时间序列） |
| 周线数据 | `get_weekly_async()` | `get_weekly()` | 周线 OHLCV 聚合 |
| 月线数据 | `get_monthly_async()` | `get_monthly()` | 月线 OHLCV 聚合 |
| 行业差异化估值 | `get_industry_valuation_async()` | `get_industry_valuation()` | 根据行业配置返回核心估值指标与分位 |
| 中国宏观周期 | `get_cn_macro_cycle_async()` | `get_cn_macro_cycle()` | 返回月度 raw/stable 宏观阶段、信用脉冲与观测/生效时间 |
| 宏观行业快照 | `get_cn_macro_cycle_industries_async()` | `get_cn_macro_cycle_industries()` | 返回申万三级行业在当前宏观阶段下的匹配快照 |

#### 🚀 SDK 使用示例

**在 Jupyter Notebook 中（推荐方式）**:

```python
from finance_data_hub.config import get_settings
from finance_data_hub import FinanceDataHub

# 初始化
settings = get_settings()
fdh = FinanceDataHub(
    settings=settings,
    backend="postgresql",
    router_config_path="sources.yml"  # 可选
)

# 直接使用 await（推荐）
daily_data = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
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

# 周线/月线数据（自动聚合）
weekly = await fdh.get_weekly_async(['600519.SH'], '2024-01-01', '2024-12-31')
monthly = await fdh.get_monthly_async(['600519.SH'], '2024-01-01', '2024-12-31')

# 中国宏观周期（月度主表，默认建议使用 stable 阶段）
macro_cycle = await fdh.get_cn_macro_cycle_async(
    start_date='2024-01-01',
    phase_mode='stable'
)
print(macro_cycle[['time', 'phase', 'credit_impulse']].tail())

# 当前宏观阶段下的优先行业列表（申万三级）
preferred_industries = await fdh.get_cn_macro_cycle_industries_async(
    preferred_only=True,
    phase_mode='stable'
)
print(preferred_industries[['time', 'l3_name', 'config_macro_cycle']].tail())

# 关闭连接
await fdh.close()
```

**在普通 Python 脚本中**:

```python
from finance_data_hub.config import get_settings
from finance_data_hub import FinanceDataHub

settings = get_settings()
fdh = FinanceDataHub(settings, backend="postgresql")

# 使用同步方法（自动处理事件循环）
daily_data = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"日线数据: {len(daily_data)} 条记录")

# 或者使用异步方式
import asyncio

async def get_data():
    daily = await fdh.get_daily_async(['600519.SH'], '2024-01-01', '2024-12-31')
    await fdh.close()
    return daily

daily_data = asyncio.run(get_data())
```

#### 📝 完整文档

详细的使用指南、API 文档和实施报告请参考：
- [Phase 3 完整文档](./docs/features/Phase3_Complete_Documentation.md) - 包含完整的实施细节、使用示例、性能指标和技术架构
- [定时下载与数据预处理设计](./docs/features/SchedulerPreprocessing.md) - 调度器、预处理类别与运行方式
- [A 股日度估值缺失补值](./docs/features/daily_valuation_fill.md) - 补值口径、CLI 工作流、SDK `filled=True` 查询方式
- [中国宏观周期预处理](./docs/features/cn_macro_cycle_preprocessing.md) - 月度宏观阶段与行业快照设计、CLI、SDK、调度配置
- [ValueInvesting 宏观周期接入指南](./docs/guides/valueinvesting_macro_cycle_integration_guide.md) - 智能选股与 qlib 特征补充的下游接入建议

#### 性能指标

| 数据类型 | 性能目标 | 实际实现 |
|----------|----------|----------|
| 日线数据 | < 200ms (2个股票，1年) | ✅ 符合 |
| 分钟数据 | < 500ms (1个股票，1月) | ✅ 符合 |
| 每日基本面 | < 300ms (2个股票，1年) | ✅ 符合 |
| 复权因子 | < 200ms (2个股票，5年) | ✅ 符合 |
| 股票基本信息 | < 100ms (10个股票) | ✅ 符合 |

### 📦 Phase 4: 调度与预处理模块 - 进行中 🔄

Phase 4 核心能力已完成，仍在持续补充端到端集成测试与下游接入文档。

#### ✅ 已完成功能

**4.1 调度模块** ✅
- ✅ 设计 `schedules.yml` 配置格式
- ✅ 实现 `scheduler/` 模块（models, engine, executor, manager）
- ✅ 实现 CLI `fdh-cli schedule` 命令

**4.2 预处理模块** ✅
- ✅ 复权处理（qfq/hfq）
- ✅ 周期重采样（weekly/monthly）
- ✅ 技术指标（MA/EMA/MACD/RSI/ATR）
- ✅ 基本面指标（估值分位/F-Score）
- ✅ 行业差异化估值（`processed_industry_valuation`）
- ✅ 中国宏观周期（`processed_cn_macro_cycle_phase` / `processed_cn_macro_cycle_industry`）
- ✅ 预处理数据表 SQL

**4.3 SDK 扩展** ✅
- ✅ `get_daily_adjusted()` - 复权数据获取
- ✅ `get_processed_daily/weekly/monthly()` - 预处理数据获取
- ✅ `get_processed_valuation_pct()` - 估值分位指标
- ✅ `get_quarterly_fundamental()` / `get_fundamental_combined()` - 季度基本面 / 合并基本面
- ✅ `get_industry_valuation()` - 行业差异化估值
- ✅ `get_cn_macro_cycle()` / `get_cn_macro_cycle_industries()` - 宏观周期
- ✅ `calculate_indicators()` - 实时指标计算

**4.4 CLI 扩展** ✅
- ✅ `fdh-cli preprocess run` - 执行预处理
- ✅ `fdh-cli preprocess status` - 查看状态
- ✅ `fdh-cli preprocess info` - 显示模块信息

**4.5 季度基本面指标分表存储** ✅
- ✅ F-Score 计算器（9 项 Piotroski 指标）
- ✅ `processed_fundamental_quality` 独立预处理表（季频与日频分开）
- ✅ `roe_yearly` 字段支持年化 ROE 计算
- ✅ 数据迁移脚本 `008_create_quarterly_fundamental.sql`、`009_add_roe_yearly.sql`

**4.6 待完成**
- 🔲 端到端集成测试补充
- 🔲 下游接入文档与示例完善

---

## 📐 数据标准化约定

### 列名统一
- 标准列: `time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adj_factor`
- 时间格式: ISO 8601 或 Pandas Timestamp
- 数值类型: 浮点数（价格、成交量）和整数（股票代码）

### 时间存储规范

所有数据统一使用 `TIMESTAMPTZ`（带时区的 timestamp）存储，时区统一为 `Asia/Shanghai` (UTC+8)：

| 数据类型 | 时间列 | 格式示例 | 说明 |
|---------|-------|---------|------|
| 日线数据 | `time` | `2024-01-02 15:00:00.000 +0800` | 交易日收盘时间 15:00 |
| 分钟数据 | `time` | `2024-01-02 10:30:00.000 +0800` | 实际交易时间 |
| 周线数据 | `time` | `2024-01-05 15:00:00.000 +0800` | 周五收盘时间 |
| 月线数据 | `time` | `2024-01-31 15:00:00.000 +0800` | 月末交易日收盘时间 |
| 季度财报 | `end_date_time` | `2024-03-31 15:00:00.000 +0800` | 季度末收盘时间 |
| 宏观数据 | `time` | `2024-03-31 15:00:00.000 +0800` | 季度末/月末收盘时间 |

**重要说明**:
1. **时区统一**: 所有数据统一使用 `Asia/Shanghai` 时区存储（即使未来支持港股/美股数据也将遵循）
2. **收盘时间标记**: 日频及以上数据统一标记为对应市场的收盘时间（中国大陆 15:00，港股 16:00）
3. **查询返回**: SDK 返回 DataFrame 中 `time` 列为 `datetime64[ns, Asia/Shanghai]` 类型
4. **日期参数**: SDK 接受 `YYYY-MM-DD` 字符串格式，内部自动转换为带时区时间戳

### Symbol 格式
- 格式: `<code>.<exchange>`
- 示例: `600519.SH`（贵州茅台沪市）、`000858.SZ`（五粮液深市）、`00700.HK`（腾讯港股）

---

## 📊 SDK 完整数据类型支持

下表列出 SDK 提供的全部查询方法（同步 / 异步配对）。

| 数据类型 | 方法 | 参数说明 |
|----------|------|---------|
| 日线 | `get_daily()` / `get_daily_async()` | symbols, start_date, end_date |
| 分钟 | `get_minute()` / `get_minute_async()` | symbols, start_date, end_date, frequency |
| 每日基本面 | `get_daily_basic()` / `get_daily_basic_async()` | symbols, start_date, end_date, filled |
| 复权因子 | `get_adj_factor()` / `get_adj_factor_async()` | symbols, start_date, end_date |
| 复权日线 | `get_daily_adjusted()` / `get_daily_adjusted_async()` | symbols, start_date, end_date, adjust (qfq/hfq/none) |
| 基本信息 | `get_basic()` / `get_basic_async()` | symbols（None 表示所有） |
| 周线 | `get_weekly()` / `get_weekly_async()` | symbols, start_date, end_date |
| 月线 | `get_monthly()` / `get_monthly_async()` | symbols, start_date, end_date |
| GDP | `get_cn_gdp()` / `get_cn_gdp_async()` | start_date, end_date（季度末日期） |
| PPI | `get_cn_ppi()` / `get_cn_ppi_async()` | start_date, end_date（月份末日期） |
| 货币供应量 | `get_cn_m()` / `get_cn_m_async()` | start_date, end_date（月份末日期） |
| PMI | `get_cn_pmi()` / `get_cn_pmi_async()` | start_date, end_date（月份末日期） |
| 指数日线行情 | `get_index_daily()` / `get_index_daily_async()` | ts_code, start_date, end_date |
| 指数每日指标 | `get_index_dailybasic()` / `get_index_dailybasic_async()` | ts_code, start_date, end_date |
| 财务指标 | `get_fina_indicator()` / `get_fina_indicator_async()` | ts_code, start_date, end_date（报告期） |
| 现金流量表 | `get_cashflow()` / `get_cashflow_async()` | ts_code, start_date, end_date（报告期） |
| 资产负债表 | `get_balancesheet()` / `get_balancesheet_async()` | ts_code, start_date, end_date（报告期） |
| 利润表 | `get_income()` / `get_income_async()` | ts_code, start_date, end_date（报告期） |
| 申万行业分类 | `get_sw_industry_classify()` / `get_sw_industry_classify_async()` | level (L1/L2/L3) |
| 申万行业成分股 | `get_sw_industry_members()` / `get_sw_industry_members_async()` | l1_code/l2_code/l3_code/ts_code |
| 申万行业日线行情 | `get_sw_daily()` / `get_sw_daily_async()` | ts_code, start_date, end_date |
| 交易日历 | `get_trade_cal()` / `get_trade_cal_async()` | exchange, start_date, end_date, is_open |
| 指数成分权重 | `get_index_weight()` / `get_index_weight_async()` | index_code, start_date, end_date, trade_date |
| 预处理日线 | `get_processed_daily()` / `get_processed_daily_async()` | symbols, start_date, end_date, indicators |
| 预处理周线 | `get_processed_weekly()` / `get_processed_weekly_async()` | symbols, start_date, end_date, indicators |
| 预处理月线 | `get_processed_monthly()` / `get_processed_monthly_async()` | symbols, start_date, end_date, indicators |
| 估值分位 | `get_processed_valuation_pct()` / `get_processed_valuation_pct_async()` | symbols, start_date, end_date, indicators |
| 季度基本面 | `get_quarterly_fundamental()` / `get_quarterly_fundamental_async()` | symbols, start_date, end_date |
| 合并基本面 | `get_fundamental_combined()` / `get_fundamental_combined_async()` | symbols, start_date, end_date, include_fscore |
| 行业差异化估值 | `get_industry_valuation()` / `get_industry_valuation_async()` | symbols, l2_names, start_date, end_date, include_exempted |
| 中国宏观周期 | `get_cn_macro_cycle()` / `get_cn_macro_cycle_async()` | start_date, end_date, phase_mode (`stable/raw`) |
| 宏观行业快照 | `get_cn_macro_cycle_industries()` / `get_cn_macro_cycle_industries_async()` | start_date, end_date, preferred_only, phase_mode |

**频率选项**（用于 `get_minute`）: `minute_1`, `minute_5`, `minute_15`, `minute_30`, `minute_60`

### 复权数据与实时技术指标计算

```python
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

fdh = FinanceDataHub(get_settings())

# 前复权 / 后复权 / 不复权
qfq = await fdh.get_daily_adjusted_async(['600519.SH'], '2024-01-01', '2024-12-31', adjust='qfq')
hfq = await fdh.get_daily_adjusted_async(['600519.SH'], '2020-01-01', '2024-12-31', adjust='hfq')
raw = await fdh.get_daily_adjusted_async(['600519.SH'], '2024-01-01', '2024-12-31', adjust='none')

# 实时计算技术指标（支持 ma_*, ema_*, macd, rsi_*, atr_*）
daily = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')
with_ind = fdh.calculate_indicators(
    daily,
    indicators=['ma_20', 'ma_60', 'macd', 'rsi_14', 'atr_14'],
    adjust='qfq',
)
```

### 预处理流水线（PreprocessPipeline）

```python
from finance_data_hub.preprocessing import PreprocessPipeline, AdjustType, ResampleFreq

pipeline = PreprocessPipeline()
result = (
    pipeline
    .set_data(raw_data)
    .adjust(AdjustType.QFQ)
    .add_indicator("ma_20")
    .add_indicator("macd")
    .add_indicator("rsi_14")
    .run()
)

# 多频率运行（日 / 周 / 月）
pipeline = PreprocessPipeline()
pipeline.set_data(raw_data)
pipeline.adjust(AdjustType.QFQ)
pipeline.add_indicators(["ma_20", "macd", "rsi_14"])
pipeline.resample(ResampleFreq.WEEKLY)
pipeline.resample(ResampleFreq.MONTHLY)
results = pipeline.run_with_resample()
```

### 数据新鲜度检查

```python
freshness = await fdh.check_data_freshness(symbols=['600519.SH'], dataset='daily')
print(freshness['available_providers'], freshness['recommendation'])
```

---

## 🛠️ CLI 完整命令参考

### 数据更新（更多数据集）

```bash
# 宏观经济数据
fdh-cli update --dataset gdp [--force] [--start-date 2020-03-31 --end-date 2024-12-31]
fdh-cli update --dataset ppi [--force] [--start-date 2020-01-31 --end-date 2024-12-31]
fdh-cli update --dataset m   [--force] [--start-date 2020-01-31 --end-date 2024-12-31]
fdh-cli update --dataset pmi [--force] [--start-date 2020-01-31 --end-date 2024-12-31]

# 指数行情 / 指标 / 成分权重
fdh-cli update --dataset index_daily       [--symbols all|000300.SH] [--start-date ... --end-date ...]
fdh-cli update --dataset index_dailybasic  [--symbols 000001.SH] [--trade-date 2024-11-27]
fdh-cli update --dataset index_weight      [--symbols all|000300.SH,000905.SH] [--trade-date 2024-06-30]

# 财务三大报表 / 财务指标（按股票）
fdh-cli update --dataset fina_indicator --symbols 600519.SH,000858.SZ
fdh-cli update --dataset cashflow        --symbols 600519.SH --start-date 2020-03-31 --end-date 2024-12-31
fdh-cli update --dataset balancesheet    --symbols 600519.SH
fdh-cli update --dataset income          --symbols 600519.SH

# 申万行业（分类 / 成分股 / 日线）
fdh-cli update --dataset sw_industry_classify
fdh-cli update --dataset sw_industry_member
fdh-cli update --dataset sw_daily [--symbols 801780.SI,801790.SI] [--trade-date 2024-06-28]

# 交易日历（7 个交易所）
fdh-cli update --dataset trade_cal [--symbols SSE,SZSE] [--start-date 2024-01-01 --end-date 2024-12-31]
```

### 调度命令（schedule）

```bash
# 列出 / 执行 / 启动 / 停止 / 状态
fdh-cli schedule list
fdh-cli schedule run --job daily_update
fdh-cli schedule run --job macro_cycle_preprocess
fdh-cli schedule start
fdh-cli schedule stop
fdh-cli schedule status
```

### 预处理命令（preprocess）

```bash
# 模块信息 / 状态
fdh-cli preprocess info
fdh-cli preprocess status

# 执行预处理
fdh-cli preprocess run [OPTIONS]
```

**预处理类别（`--category`）**:
- `technical`: 技术指标（MA, MACD, RSI, ATR）
- `fundamental`: 日频基本面指标（估值分位）
- `quarterly_fundamental`: 季度基本面指标（F-Score、roe_5y_avg、ni_cfo_corr_3y 等）
- `industry_valuation`: 行业差异化估值（按 `industry_config.json` 自动选择 PE/PB/PS/PEG）
- `macro_cycle`: 中国宏观周期（月度主表 + 申万三级行业快照）
- `all`: 全部类别

**关键选项**:

| 选项 | 说明 |
|------|------|
| `--all, -a` | 处理全部股票 |
| `--symbols, -s` | 股票代码列表（逗号分隔） |
| `--market` | 市场（`CN` / `HK` / `ALL`，默认 `CN`） |
| `--freq, -f` | 频率（`daily,weekly,monthly`） |
| `--adjust` | 复权类型（`qfq` / `hfq` / `none`） |
| `--start-date` / `--end-date` | 日期范围 |
| `--force` | 强制全量重算 |
| `--batch-size, -b` | 批处理大小 |
| `--max-concurrent, -C` | 最大 I/O 并发批次数 |
| `--num-workers, -w` | 进程池工作进程数（CPU 并发，0=自动） |
| `--verbose, -v` | 详细日志 |

**支持的技术指标**: `ma_5/10/20/60/120/250`, `macd`（dif/dea/hist）, `rsi_6/14`, `atr_14`

**预处理数据表**:

| 表名 | 频率 | 说明 |
|------|------|------|
| `processed_daily_qfq` / `processed_weekly_qfq` / `processed_monthly_qfq` | 日 / 周 / 月 | 前复权 OHLCV + 技术指标 |
| `processed_valuation_pct` | 日频 | 估值分位等日频基本面指标 |
| `processed_fundamental_quality` | 季频 | F-Score 及财务质量指标 |
| `processed_industry_valuation` | 日频 | 行业差异化估值指标 |
| `processed_cn_macro_cycle_phase` | 月频 | 宏观周期主表（raw/stable + 信用脉冲 + 生效月） |
| `processed_cn_macro_cycle_industry` | 月频 | 宏观周期行业快照（申万三级） |

**典型用法**:

```bash
# 首次全量预处理
fdh-cli preprocess run --all --category all --force

# 日常增量更新（智能检测复权变动）
fdh-cli preprocess run --all --category technical

# 港股技术指标
fdh-cli preprocess run --all --category technical --market HK
fdh-cli preprocess run --category technical --market HK --symbols 00700.HK,00005.HK

# 季度基本面 / 行业差异化估值 / 宏观周期
fdh-cli preprocess run --all --category quarterly_fundamental
fdh-cli preprocess run --all --category industry_valuation
fdh-cli preprocess run --category macro_cycle

# 高性能并发
fdh-cli preprocess run --all --category technical --max-concurrent 10 --num-workers 8
```

**注意事项**:
- 默认 `--market CN`；非 `CN` 市场仅支持 `technical`
- `macro_cycle` 不按股票粒度运行，`--symbols` 会被忽略；依赖 `cn_m`、`cn_ppi`、`cn_pmi`、`cn_gdp`、`sw_industry_member`
- 技术指标需要足够历史数据（如 MA250 至少 250 天）
- 基本面指标依赖 `daily_basic` 表中的 PE/PB/PS

### 数据预处理 SDK 查询示例

```python
# 预处理日 / 周 / 月线（含技术指标）
daily = await fdh.get_processed_daily_async(
    symbols=['600519.SH'], start_date='2024-01-01', end_date='2024-12-31',
    indicators=['ma_20', 'macd_dif', 'rsi_14', 'atr_14'],
)
weekly  = await fdh.get_processed_weekly_async(['600519.SH'], '2024-01-01', '2024-12-31')
monthly = await fdh.get_processed_monthly_async(['600519.SH'], '2024-01-01', '2024-12-31')

# 估值分位 + F-Score
val = await fdh.get_processed_valuation_pct_async(
    ['600519.SH'], '2024-01-01', '2024-12-31',
    indicators=['pe_ttm_pct_1250d', 'pb_pct_1250d', 'peg', 'f_score'],
)

# 季度基本面（F-Score 等）
q = await fdh.get_quarterly_fundamental_async(['600519.SH'], '2020-01-01', '2024-12-31')

# 合并基本面（日度估值 + 季度 F-Score）
combined = await fdh.get_fundamental_combined_async(
    ['600519.SH'], '2024-01-01', '2024-12-31', include_fscore=True,
)

# 行业差异化估值（自动选择 PE/PB/PS/PEG）
iv = await fdh.get_industry_valuation_async(start_date='2024-01-01', end_date='2024-12-31')

# 中国宏观周期（默认推荐 stable）
macro = await fdh.get_cn_macro_cycle_async(start_date='2020-01-01', phase_mode='stable')
industries = await fdh.get_cn_macro_cycle_industries_async(preferred_only=True, phase_mode='stable')
```

### 配置管理

```bash
fdh-cli config show     # 查看当前配置
fdh-cli config test     # 测试配置（数据库 / Tushare / Redis 连接）
```

### 数据库清理

```bash
# 完全清理（删除所有表 / 视图 / 函数 / 连续聚合，需确认）
fdh-cli cleanup --mode all
fdh-cli cleanup --mode all --yes        # 跳过确认
fdh-cli cleanup --mode data_only        # 只清空数据，保留表结构
fdh-cli cleanup --mode aggregates       # 只删除连续聚合视图
fdh-cli cleanup --mode all --verbose    # 显示详细信息
```

**⚠️ 注意**: 数据库清理操作不可逆，请谨慎使用。

---

## 重要设计决策

1. **智能路由**: 通过 `sources.yml` 配置驱动数据源选择，支持故障转移
2. **冷热分离**: PostgreSQL 作为源数据存储，Parquet+DuckDB 作为分析存储
3. **微服务架构**: XTQuant 作为独立微服务，通过 HTTP API 集成
4. **事件驱动**: Redis Pub/Sub 作为实时数据流总线
5. **配置即代码**: 使用 Pydantic 实现类型安全的配置管理

---

## XTQuant 集成注意事项

- XTQuant 仅支持 Windows 且依赖 QMT
- 通过 HTTP API 客户端模式集成：`XTQuantProvider` → `xtquant_helper` 微服务
- `xtquant_helper` 必须提供：
  1. REST API 用于批量数据请求
  2. WebSocket 接口用于实时行情推送
- 微服务地址在 `sources.yml` 中配置

---

### 下一步 - Phase 5 (规划中)

- 🔲 **完整ETL** - PostgreSQL → Parquet + DuckDB
  - 数据提取器、转换器、Parquet 写入器、DuckDB 查询优化

- 🔲 **流式处理** - WebSocket 实时数据
  - 实时数据订阅、Redis Pub/Sub 集成、归档服务、实时行情看板

- 🔲 **测试与部署**
  - 端到端集成测试、Dockerfile 优化、性能测试
