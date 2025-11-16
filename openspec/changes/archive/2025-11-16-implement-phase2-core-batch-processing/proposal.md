# Phase 2: 核心批处理流程实现

## Why
Phase 1 已经完成了基础设施的搭建（配置管理、CLI框架、Docker服务）。现在需要实现核心的数据处理流程，包括数据源适配、智能路由、数据存储和ETL功能，为后续的数据访问SDK和流式处理奠定基础。

Phase 2 是整个系统的核心组件，将实现从数据源到存储的完整数据管道，支持多种数据源和智能路由。

## What Changes

### 1. 数据提供者适配器层
- 实现 `TushareProvider` (基于直接API调用)
- 实现 `XTQuantProvider` (作为 xtquant_helper 微服务的HTTP API客户端)
- 统一数据提供者接口，返回标准化的DataFrame格式
- 支持多种数据频率：TushareProvider支持daily；XTQuantProvider支持daily、minute_1、minute_5、tick
- 实现错误处理和重试机制

### 2. 智能数据源路由系统
- 实现 `sources.yml` 配置文件结构
- 开发基于配置的数据源路由器
- 支持按资产类别、数据频率进行路由决策
- 实现故障转移机制
- 支持数据源的优先级和权重配置

### 3. 数据库表结构设计
- 设计标准化的时间序列数据表结构
- 实现资产基本信息表 (`asset_basic`)
- 实现行情数据表 (`symbol_daily`, `symbol_minute`)
- 实现每日指标数据表(`daily_basic`)
- 使用 TimescaleDB 的超表 (Hypertable) 特性优化查询性能
- 创建适当的索引和分区策略

### 4. CLI 核心命令实现
- 完善 `fdh-cli update` 命令：
  - 集成智能数据源路由器
  - 支持增量更新和全量更新模式
  - 实现 "数据源 -> TimescaleDB" 的完整流程
  - 添加进度显示和错误处理

- 完善 `fdh-cli etl` 命令：
  - 实现 "TimescaleDB -> Parquet+DuckDB" 的同步流程
  - 支持按日期和股票代码进行选择性ETL
  - 实现批处理和压缩优化
  - 添加ETL进度跟踪

### 5. 数据标准化与验证
- 定义统一的列名格式：`time`, `symbol`, `open`, `high`, `low`, `close`, `volume`, `amount`, `adj_factor`, `open_interest`, `settle`
- 实现数据验证和清洗逻辑
- 处理时区转换和时间格式统一
- 实现缺失值处理

## Impact
- **Affected Specs**: 数据提供者、智能路由、CLI更新、数据存储
- **Affected Code**:
  - `finance_data_hub/providers/`: 数据提供者模块
  - `finance_data_hub/storage/`: 存储和数据库模块
  - `finance_data_hub/utils/router.py`: 智能路由器
  - `sources.yml`: 数据源配置文件
  - `sql/init/`: 数据库初始化脚本
  - `fdh-cli update/etl`: CLI命令实现
- **Breaking Changes**: 无（增强现有功能）
- **Dependencies**: Tushare API、XTQuant微服务、PostgreSQL、TimescaleDB

## Success Criteria
1. TushareProvider 能成功获取并存储日线数据
2. XTQuantProvider 能成功调用微服务并获取数据
3. 智能路由器能根据配置选择正确的数据源
4. `fdh-cli update` 能完成完整的数据更新流程
5. `fdh-cli etl` 能成功将数据导出到Parquet格式
6. 数据库表结构正确创建并可查询
7. 所有组件通过单元测试和集成测试

## 技术要点

### TushareProvider
- 使用 `tushare` Python库进行API调用
- 实现Token认证和限频控制
- 支持批量股票代码处理
- 返回标准化的pandas DataFrame

### XTQuantProvider
- 作为HTTP客户端调用 xtquant_helper 微服务
- 微服务运行在 `http://localhost:8100`
- 支持多种数据获取API：`get_market_data`, `get_local_data` 等
- 处理DataFrame和numpy数组的转换

### 数据源路由器
- 基于 Yaml 配置的动态路由
- 支持路由策略：故障转移、负载均衡、优先级
- 运行时可重配置
- 性能监控和指标收集

### 数据库设计
- 使用TimescaleDB的超表特性
- 按symbol和时间进行分区
- 创建复合索引优化查询性能
- 实现数据压缩和归档策略

### ETL流程
- 使用DuckDB作为查询引擎
- 实现高效的批量读取和写入
- 使用Parquet的Zstd压缩
- 支持增量ETL和全量ETL
