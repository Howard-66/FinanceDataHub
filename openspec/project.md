# 项目上下文

## 项目目标与愿景
**FinanceDataHub** 是一个综合性金融数据服务中心，旨在为量化投资和金融分析提供统一、可靠、高性能的数据基础。

### 核心目标
- 构建开放且可扩展的数据架构，轻松接入新的数据源和存储引擎
- 实现服务解耦，严格分离数据获取、存储、访问和流处理
- 性能优先，采用列式存储和内存计算技术处理高频金融数据
- 提供开发者友好的API，专注于数据使用而非数据获取
- 确保数据一致性，明确的数据更新机制（增量/全量）
- 完整的可观测性，日志记录和性能监控

### 应用场景
- 行情显示系统
- 量化分析平台
- 投资研究工具
- 策略回测框架
- 实时数据流处理
- AI/ML 模型训练数据支撑

## 技术栈

### 核心技术
- **编程语言**: Python 3.10+
- **配置管理**: Pydantic + .env
- **CLI框架**: Typer 或 Click
- **主数据库**: PostgreSQL 16 + TimescaleDB
- **分析引擎**: DuckDB
- **文件格式**: Parquet (Zstd压缩)
- **调度器**: APScheduler 或 Crontab
- **流处理**: Redis Pub/Sub
- **ORM/DB驱动**: SQLAlchemy (Core) + asyncpg
- **日志**: Loguru
- **部署**: Docker Compose
- **微服务框架**: FastAPI (用于 xtquant_helper)

### 数据源集成
- **Tushare**: 批量历史数据获取
- **XTQuant**: 实时行情流（通过 xtquant_helper 微服务）

### 集成生态
- **Qlib**: 量化投资框架
- **FinRL**: 强化学习交易框架
- **Jupyter**: 交互式分析环境
- **WebSocket**: 实时看板系统

## 项目约定

### 代码风格
- **PEP 8** Python编码规范
- **Black** 代码格式化（行长度：88字符）
- **isort** 导入排序
- **类型提示**: 强制使用 Python 类型注解
- **文档字符串**: 使用 Google 风格 docstring，包含中文注释
- **变量命名**:
  - 模块: 小写字母，下划线分隔 (e.g., `data_provider.py`)
  - 类: PascalCase (e.g., `TushareProvider`)
  - 函数/变量: 小写字母，下划线分隔 (e.g., `get_daily_data`)
  - 常量: 全大写字母，下划线分隔 (e.g., `MAX_RETRY_COUNT`)

### 架构模式

#### 1. 分层架构
- **应用层**: 上层应用和SDK
- **数据访问层**: SDK + 智能路由
- **存储层**: Hot/Cold 分离
- **核心服务层**: CLI + 调度器
- **数据源层**: Provider 适配器
- **流处理总线**: Pub/Sub 消息中间件

#### 2. 适配器模式 (Provider Layer)
- `TushareProvider`: 适配 Tushare API
- `XTQuantProvider`: 适配 xtquant_helper 微服务
- 统一接口，标准化 DataFrame 输出

#### 3. 智能路由 (Smart Routing)
- 基于 `sources.yml` 配置的动态路由
- 支持故障转移和负载均衡
- 根据资产类别和数据频率自动选择最优数据源

#### 4. 冷热分离 (Hot/Cold Separation)
- **Hot Storage** (PostgreSQL + TimescaleDB): 实时数据，主存储
- **Cold Storage** (Parquet + DuckDB): 历史数据，分析优化

### 测试策略
- **测试框架**: pytest
- **单元测试**:
  - Provider 适配器测试
  - 智能路由逻辑测试
  - CLI 命令测试
  - 数据格式化测试
- **集成测试**:
  - 数据库连接测试
  - ETL 流程测试
  - API 集成测试
- **模拟对象**: 使用 `unittest.mock` 和 `pytest-mock`
- **测试覆盖**: 目标覆盖率达到 80%+
- **测试数据**: 使用 fixture 和 factory 生成模拟数据

### Git 工作流

#### 分支策略
- **主分支**: `main` - 稳定可发布版本
- **开发分支**: `develop` - 日常开发集成
- **功能分支**: `feature/<feature-name>` - 新功能开发
- **修复分支**: `hotfix/<issue-id>` - 紧急修复
- **发布分支**: `release/<version>` - 发布准备

#### 提交信息规范
使用 **Conventional Commits** 格式：
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**类型 (type)**:
- `feat`: 新功能
- `fix`: 缺陷修复
- `docs`: 文档更新
- `style`: 代码格式调整（不影响功能）
- `refactor`: 重构
- `test`: 测试相关
- `chore`: 构建/工具/依赖更新

**示例**:
```
feat(provider): add tushare daily data fetcher

- implement get_daily method
- support batch symbol processing
- add error handling

Closes #123
```

## 领域上下文

### 金融数据术语
- **Symbol**: 证券代码，格式为 `<code>.<exchange>` (e.g., `600519.SH`)
- **Exchange**: 交易所代码 (SH=上交所, SZ=深交所)
- **K线频率**:
  - `daily`: 日线
  - `minute_1`: 1分钟线
  - `minute_5`: 5分钟线
  - `tick`: 逐笔数据
- **调整因子** (adj_factor): 用于复权计算的调整系数
- **增量更新**: 基于时间戳的增量数据同步
- **全量更新**: 强制全表刷新

### 数据标准化
- **统一列名**: `time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adj_factor`
- **时间格式**: ISO 8601 (e.g., `2024-01-01 09:30:00`)
- **数值精度**: 价格保留4位小数，成交量为整数
- **缺失值**: 使用 `None` 或 `NaN`，严禁空字符串

### XTQuant 特殊说明
- 仅支持 Windows 平台，依赖 QMT 量化交易软件
- 通过 `xtquant_helper` 微服务间接集成
- 微服务需提供:
  1. REST API 批量数据接口
  2. WebSocket 实时行情推送接口
- API 地址在 `sources.yml` 中配置

## 重要约束

### 技术约束
1. **Python 版本**: 必须使用 Python 3.10 或更高版本
2. **数据库兼容性**: PostgreSQL 版本需为 16+
3. **TimescaleDB**: 必须是开源版本或企业版
4. **XTQuant 限制**: 仅 Windows 环境可用，需要 QMT 软件

### 数据完整性约束
1. **主键约束**: 时间序列数据使用 `(symbol, time)` 作为复合主键
2. **事务一致性**: 所有数据库写入必须在事务中完成
3. **幂等性**: Provider 写入操作必须支持重复执行
4. **增量标识**: 每条记录需包含 `updated_at` 时间戳

### 性能约束
1. **延迟要求**: 实时数据延迟 < 100ms
2. **吞吐量**: 支持 1000+ 股票并发写入
3. **查询延迟**: 复杂分析查询 < 5s (DuckDB)
4. **存储增长**: 日数据增量 < 1GB

### 安全约束
1. **API 密钥**: 必须通过环境变量管理，严禁硬编码
2. **数据库密码**: 使用强密码或认证文件
3. **微服务通信**: 生产环境使用 HTTPS
4. **访问控制**: CLI 工具支持多环境配置隔离

## 外部依赖

### 数据源服务
1. **Tushare Pro**
   - API 文档: https://tushare.pro/document/2
   - 认证: Token (环境变量 `TUSHARE_TOKEN`)
   - 限频: 根据账户等级限制调用频率
   - 费用: 免费版每日限额，付费版解锁更多

2. **XTQuant (QMT)**
   - 开发者: 迅投
   - 平台: 仅 Windows
   - 集成方式: 通过 xtquant_helper 微服务
   - 实时性: 毫秒级行情数据

### 基础设施服务
1. **PostgreSQL 17 + TimescaleDB**
   - 用途: 主存储数据库
   - 连接: 使用 asyncpg 异步驱动
   - 配置: 需要启用 TimescaleDB 扩展

2. **DuckDB**
   - 用途: OLAP 分析引擎
   - 集成: 通过 Python API (duckdb)
   - 数据格式: 直接读取 Parquet 文件

3. **Redis**
   - 用途: Pub/Sub 消息中间件
   - 版本: 6.0+
   - 配置: 持久化可选，建议启用 AOF

### 部署依赖
1. **Docker & Docker Compose**
   - 用途: 服务编排和环境隔离
   - 版本: Docker 20.10+, Compose 2.0+
   - 镜像: 官方 Python 基础镜像

2. **FastAPI (xtquant_helper)**
   - 用途: 提供 xtquant HTTP API 代理
   - 文档: https://fastapi.tiangolo.com/
   - 部署: 独立服务，Windows 主机运行

### Python 依赖包
核心依赖 (在 requirements.txt 中定义):
- `pydantic`: 配置管理和数据验证
- `typer` 或 `click`: CLI 框架
- `sqlalchemy`: ORM 和 SQL 工具包
- `asyncpg`: PostgreSQL 异步驱动
- `pandas`: 数据处理
- `pyarrow`: Parquet 文件支持
- `duckdb`: 分析引擎
- `redis`: 消息队列客户端
- `loguru`: 日志库
- `apscheduler`: 任务调度
- `pytest`: 测试框架
- `black`, `isort`, `flake8`: 代码质量工具
