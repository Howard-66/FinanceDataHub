## 为什么
当前的 FinanceDataHub SDK（finance_data_hub/sdk.py）仅提供高周期数据查询（周线、月线），缺少最常用的日线和分钟线数据查询，以及其他基础数据类型如每日基本面、复权因子和股票基本信息。这导致用户无法通过 SDK 访问大部分存储在数据库中的数据。此外，SmartRouter 未完全集成到 SDK 中，限制了数据访问的智能化和灵活性。

当前的 CLI update 命令支持多种数据类型：
- daily（日线行情）
- minute_*（各种分钟线）
- daily_basic（每日基本面）
- adj_factor（复权因子）
- basic（股票基本信息）

但 SDK 缺少对这些数据类型的查询接口。

## 变更内容
- 在 FinanceDataHub SDK 中添加以下数据查询方法（同步和异步版本）：
  - `get_daily()` - 日线 OHLCV 数据查询
  - `get_minute()` - 分钟级数据查询（支持1分钟、5分钟、15分钟、30分钟、60分钟）
  - `get_daily_basic()` - 每日基本面指标查询
  - `get_adj_factor()` - 复权因子查询
  - `get_basic()` - 股票基本信息查询（非时间序列）
- 将 SmartRouter 集成到 SDK 中实现数据源智能选择和可用性检查
- 在 DataOperations 类中添加所有缺失的 PostgreSQL 数据查询方法
- 更新 SDK 以简化后端管理，专注于 PostgreSQL 主存储

## 影响范围
- 影响的规范：
  - `smart-routing`：将完全集成到 SDK 数据访问层
  - `database-schema`：将添加新的查询功能（现有架构已支持）

- 影响的代码：
  - `finance_data_hub/sdk.py`：添加所有数据类型的查询方法，集成智能路由
  - `finance_data_hub/database/operations.py`：添加日线、分钟线、每日基本面、复权因子、基本信息查询方法
  - `finance_data_hub/router/smart_router.py`：连接 SDK 数据访问
