# Phase 1: 环境搭建与配置管理

## Why
FinanceDataHub 项目目前处于设计阶段，需要开始实际的代码实现。Phase 1 是整个项目的基础设施搭建，包括容器化服务、配置管理系统和CLI工具框架，为后续的数据源适配、存储层实现和数据访问SDK开发奠定基础。

## What Changes

### 1. Docker Compose 服务完善
- 完善现有的 `docker-compose.yml`，添加Redis服务（Pub/Sub消息中间件）
- 确保TimescaleDB和Redis服务正常运行并可连接
- 验证服务间的网络通信和持久化存储

### 2. Pydantic配置模块
- 创建基于Pydantic的配置管理系统 (`config.py`)
- 支持从环境变量和`.env`文件加载配置
- 配置项包括：
  - 数据库连接配置 (PostgreSQL/TimescaleDB)
  - Redis连接配置
  - 数据源API密钥 (Tushare, XTQuant)
  - 智能路由策略配置

### 3. fdh-cli 基础框架
- 使用Typer框架创建CLI工具 (`fdh-cli`)
- 实现基本命令结构：
  - `update`: 从数据源更新数据
  - `etl`: 执行ETL流程
  - `status`: 查看数据状态和监控信息
- 集成配置加载和验证逻辑

### 4. 项目结构与Python包配置
- 创建标准Python项目结构
- 创建 `pyproject.toml` 定义项目依赖和构建设置
- 设置基本的模块初始化文件
- 配置 CLI 入口点 (`fdh-cli`)

### 5. uv 虚拟环境与依赖管理
- 使用 `uv` 进行依赖管理和虚拟环境配置
- 创建和配置 `pyproject.toml` 包含所有核心依赖：
  - pydantic, typer, sqlalchemy
  - asyncpg, redis, loguru
  - pandas, pyarrow, duckdb
- 创建开发依赖组 (pytest, black, isort, flake8)
- 使用 `uv sync` 同步依赖并创建 `uv.lock`
- 配置 `.python-version` 指定 Python 3.10+

## Impact
- **Affected Specs**: 配置管理系统、CLI工具架构
- **Affected Code**:
  - `docker-compose.yml`: 容器编排配置
  - `config.py`: 配置管理模块
  - `cli/`: CLI工具目录
  - `pyproject.toml`: 项目配置和依赖管理
  - `uv.lock`: 依赖锁定文件
- **Breaking Changes**: 无（全新功能）
- **Dependencies**: Docker, Docker Compose, Python 3.10+, uv（包管理）

## Success Criteria
1. `docker-compose up -d` 成功启动所有服务
2. `uv sync` 成功安装所有依赖并创建虚拟环境
3. 可以通过 `fdh-cli --help` 查看帮助信息
4. 配置模块能够正确加载和验证环境变量
5. 所有服务可通过localhost正常访问
