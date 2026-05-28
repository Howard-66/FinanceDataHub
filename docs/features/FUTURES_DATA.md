# 期货数据支持说明

本文档描述 FinanceDataHub 当前已经落地的期货数据支持范围、代码规范、数据库表结构、CLI/SDK 入口，以及已完成的真实链路验证结果。

## 当前状态

期货数据 V1 已接入当前 PostgreSQL/TimescaleDB 实例，使用独立 `futures` schema 存储，`public` schema 中的股票、指数、宏观、调度和通用表保持不变。

当前已实现的数据集：

- 合约基础信息
- 主力/连续合约映射
- 日线
- 分钟线
- 结算参数
- 南华指数日线
- 现货基差
- 注册仓单
- 期限结构
- 跨期价差
- 展期收益率

当前未实现或明确排除：

- 近月合约数据集
- Tushare `fut_wsr` 仓单日报
- AKShare `get_roll_yield_bar` 直接落库

## 数据源策略

优先级固定为 `Tushare > XtQuant > AKShare`。

当前路由策略：

- `future/basic` -> `tushare`
- `future/mapping` -> `tushare`
- `future/daily` -> `tushare`, 失败后 fallback 到 `xtquant`
- `future/minute` -> `xtquant`
- `future/settle` -> `tushare`
- `future/index_daily` -> `tushare`
- `future/spot_basis` -> `akshare`
- `future/inventory` -> `akshare`

配置样例见 [sources.yml.example](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sources.yml.example:1)。

## 代码与交易所规范

### 交易所代码

FinanceDataHub 内部统一保存规范交易所代码：

| 交易所 | 内部规范 | XtQuant | Tushare 后缀 |
| --- | --- | --- | --- |
| 上期所 | `SHFE` | `SF` | `SHF` |
| 大商所 | `DCE` | `DF` | `DCE` |
| 郑商所 | `CZCE` | `ZF` | `ZCE` |
| 中金所 | `CFFEX` | `IF` | `CFX` |
| 能源中心 | `INE` | `INE` | `INE` |
| 广期所 | `GFEX` | `GF` | `GFE` |

说明：

- `contract_basic.exchange`、`daily.exchange` 等字段存储 `SHFE/DCE/CZCE/...`
- 合约代码存储使用 Tushare 风格后缀，如 `RB2405.SHF`
- Provider 边界会负责 `SHFE <-> SHF <-> SF` 之类的转换

### 合约代码

统一存储格式：

- 普通合约：`RB2405.SHF`
- 主力合约：`RB.SHF`
- 连续合约：`RBL.SHF`

与 XtQuant 的映射关系：

- `RB2405.SHF` <-> `rb2405.SF`
- `RB.SHF` <-> `rb00.SF`
- `RBL.SHF` <-> `rbL0.SF`

### 重要实现细节

- `quote_unit_desc` 中的数值部分会被提取到 `quote_unit_value`
- 存储符号统一使用 Tushare 风格，查询时允许传入 XtQuant 风格，系统会自动规范化
- Tushare 某些接口的 `exchange` 请求参数真实测试后确认应使用 `SHFE/GFEX` 这类标准交易所代码；合约后缀仍保持 `SHF/GFE`

## 数据库结构

SQL 初始化文件：

- [sql/init/008_create_futures_schema.sql](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sql/init/008_create_futures_schema.sql:1)
- [sql/init/009_create_futures_minute_aggregates.sql](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/sql/init/009_create_futures_minute_aggregates.sql:1)

`futures` schema 当前包含：

- 普通表：`contract_basic`
- Hypertable：`contract_mapping`
- Hypertable：`daily`
- Hypertable：`minute_1m`（1 分钟原始数据）
- Hypertable：`minute_5m`（5 分钟原始数据，直接从 XTQuant `period='5m'` 获取）
- Continuous Aggregate：`minute_15m`
- Continuous Aggregate：`minute_30m`
- Continuous Aggregate：`minute_60m`
- Legacy Hypertable：`minute`（旧版带 `frequency` 的分钟表，保留用于迁移兼容）
- Hypertable：`settle`
- Hypertable：`index_daily`
- Hypertable：`spot_basis`
- Hypertable：`inventory_receipt`
- Hypertable：`term_metrics`

交易日历继续复用 `public.trade_cal`，并已支持 `GFEX` 增量写入。

`inventory_receipt` 当前按品种和日期保存聚合库存，仅保留 `time`、`product_code`、`inventory`、`source`；交易所、仓库、地区等明细字段不再作为表结构字段。

## Provider 实现

### Tushare

实现位置：[tushare.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/providers/tushare.py:3177)

已接入接口：

- `fut_basic`
- `fut_mapping`
- `fut_daily`
- `fut_settle`
- `index_daily`
- `trade_cal`

实现特征：

- 处理 `fut_daily` 2000 条限制
- 处理 `fut_settle` 1600 条限制
- 自动补齐 `product_code`、`exchange`、`contract_type`
- `trade_cal` 返回值中的交易所代码会统一标准化为 `SHFE/GFEX/...`

### XtQuant

实现位置：[xtquant.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/providers/xtquant.py:733)

已接入：

- 期货日线
- 1m 分钟线原始下载

调用方式：

- 通过现有 `xtquant_helper` HTTP API
- 使用 `/download_history_data` 和 `/get_local_data`
- CLI/SDK 内部原始下载写入 `1m` 和 `5m`。`5m` 直接通过 XTQuant `period='5m'` 获取；`15m`、`30m`、`60m` 由 TimescaleDB continuous aggregate 从 `minute_5m` 派生

### AKShare

实现位置：[akshare_provider.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/providers/akshare_provider.py:1)

已接入：

- `futures_spot_price`
- `futures_spot_price_daily`
- `get_receipt`
- `futures_inventory_99`

实现特征：

- 基差统一按 `spot_price - futures_price` 重算
- `get_receipt` 按 5 天窗口切片调用
- `futures_inventory_99` 作为历史初始化补充来源
- 库存表保存品种日期级聚合值，`get_receipt` 的 `receipt` 数值会归一到 `inventory`

## 预处理逻辑

实现位置：[updater.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/update/updater.py:2813)

已实现输出：

- `term_metrics`：期限结构、跨期价差和展期收益率的统一快照表

数据来源：

- 直接读取 `futures.daily`
- 结合 `futures.contract_basic`
- 主力月份配置优先读取 `finance_data_hub/preprocessing/futures/variety.json`
- 若本仓库内未提供，则回退读取并列项目 `../futures_nexus/setting/variety.json`

当前实现思路：

- 期限结构 `flag` 根据候选曲线的升贴水方向计算
- 跨期价差记录主力/次主力合约及 `primary_close - secondary_close`
- 展期收益率使用主次合约价格和到期日差计算年化值，不调用 AKShare 展期收益率接口

## CLI 用法

实现位置：[main.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/cli/main.py:449)

示例：

```bash
fdh-cli update --asset-class future --dataset basic
fdh-cli update --asset-class future --dataset daily --symbols all --start-date 2024-04-01 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset mapping --symbols RB.SHF --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset daily --symbols RB2405.SHF --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset minute_1 --symbols rb2405.SF --start-date 2024-04-30 09:30:00 --end-date 2024-04-30 10:00:00
fdh-cli update --asset-class future --dataset minute_5 --symbols rb2405.SF --start-date 2024-04-30 09:30:00 --end-date 2024-04-30 10:00:00
fdh-cli refresh-aggregates futures.minute_15m --start 2024-04-30 --end 2024-05-01
fdh-cli refresh-aggregates futures.minute_30m --start 2024-04-30 --end 2024-05-01
fdh-cli refresh-aggregates futures.minute_60m --start 2024-04-30 --end 2024-05-01
fdh-cli update --asset-class future --dataset settle --symbols RB2405.SHF --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset index_daily --symbols NHCI.NH --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset spot_basis --symbols RB --trade-date 2024-04-30
fdh-cli update --asset-class future --dataset inventory --symbols RB --start-date 2024-04-30 --end-date 2024-04-30
fdh-cli update --asset-class future --dataset term_metrics --symbols RB --start-date 2024-04-30 --end-date 2024-04-30
```

说明：

- `--symbols` 对于行情/结算/映射场景可传合约代码
- `--symbols` 对于 `spot_basis`、`inventory`、预处理场景可直接传品种代码，例如 `RB`
- `--symbols all` 已支持，用于按 `futures.contract_basic` 展开全量合约池
- 输入 `rb2405.SF` 这类 XtQuant 风格代码时，会自动转换到内部标准格式
- 期货分钟线只下载 `minute_1` 原始数据。`minute_5`、`minute_15`、`minute_30`、`minute_60` 为连续聚合查询结果，不再调用 Provider 下载

`--symbols all` 的当前定义：

- `daily`：普通合约 + 主力合约 + 连续合约
- `settle`：普通合约
- `mapping`：主力合约 + 连续合约
- `minute`：显式 `all` 时展开普通合约 + 主力合约 + 连续合约；不指定 `--symbols` 时默认仅更新当前主力合约
- `spot_basis` / `inventory` / `term_metrics`：表示全部品种

合约池获取与增量判断：

- 全量合约池统一来自 `futures.contract_basic`
- 普通合约是否纳入下载，使用 `list_date` 与 `delist_date/last_ddate` 和请求时间窗做重叠判断
- 因此当使用 `--symbols all --start-date ... --end-date ...` 时，会自动覆盖该时间范围内仍在交易或历史上曾经有效的普通合约
- 主力/连续合约记录同样来自 `contract_basic`，其 `contract_type` 分别为 `main`、`continuous`

## SDK 用法

实现位置：[sdk.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/finance_data_hub/sdk.py:2372)

当前可用方法：

- `get_futures_contracts`
- `get_futures_daily`
- `get_futures_minute`
- `get_futures_spot_basis`
- `get_futures_inventory`
- `get_futures_term_metrics`

示例：

```python
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

fdh = FinanceDataHub(get_settings())

contracts = fdh.get_futures_contracts(product_codes=["RB"])
daily = fdh.get_futures_daily(symbols=["RB2405.SHF"], start_date="2024-04-30", end_date="2024-04-30")
minute = fdh.get_futures_minute(symbols=["rb2405.SF"], start_date="2024-04-30 09:30:00", end_date="2024-04-30 10:00:00", frequency="1m")
minute_15 = fdh.get_futures_minute(symbols=["rb2405.SF"], start_date="2024-04-30 09:30:00", end_date="2024-04-30 15:00:00", frequency="15m")
basis = fdh.get_futures_spot_basis(product_codes=["RB"], start_date="2024-04-30", end_date="2024-04-30")
inventory = fdh.get_futures_inventory(product_codes=["RB"], start_date="2024-04-30", end_date="2024-04-30")
```

## 真实验证结果

以下验证已经在当前仓库和当前数据库实例上跑通：

### 数据库

- `futures` schema 全部 11 张表已存在
- 10 张时间序列表已注册为 hypertable
- 通过 `DataOperations` 完成真实写入、upsert、查询和清理
- `term_metrics` 已通过真实样本生成成功
- `contract_basic` 真实合约分布已确认：`normal=3493`，`main=19`，`continuous=21`
- `--symbols all` 的真实 contract pool 展开已验证：
  - `daily` 在 `2024-04-01 ~ 2024-04-30` 时间窗内展开 `271` 个合约
  - `settle` 同时间窗内展开 `231` 个普通合约
  - `mapping` 同时间窗内展开 `40` 个主力/连续合约

### 期限结构预处理

- 已使用真实 Tushare 数据对 `RB` 品种执行完整预处理链路：
  - 交易日：`2024-04-30`
  - 日线样本：`SHFE` 当日整批拉取后筛出 `RB`，共 14 条
  - 结算参数样本：当日整批拉取后筛出 `RB`，共 12 条
- 预处理结果已真实写入数据库：
  - `term_metrics`：1 条
- 当次真实结果中，主/次合约为 `RB2410.SHF` / `RB2501.SHF`，价差为 `2.0`
- 已新增可重复执行的集成测试：
  [test_futures_term_preprocess_integration.py](/Volumes/Repository/Projects/TradingNexus/FinanceDataHub/tests/integration/test_futures_term_preprocess_integration.py:1)

### AKShare

- `spot_basis` 真实调用成功
- `inventory` 真实调用成功
- CLI 已用真实数据将 `RB` 的基差和仓单写入数据库

### XtQuant

- 本地 `xtquant_helper` 可初始化，`health_check=True`
- 分钟线和日线的 provider 边界逻辑已通过单元测试验证
- 分钟频率已确认限制为 `1m`、`5m`、`1h`，不接受 `15m`、`30m`

### Tushare

- `.env` 中的 `TUSHARE_TOKEN` 已成功加载
- `fut_basic(exchange="SHFE")` 实测返回 `3533` 条
- `fut_daily(symbol="RB2405.SHF", date="2024-04-30")` 实测成功
- `fut_settle(symbol="RB2405.SHF", date="2024-04-30")` 实测成功
- `fut_mapping(symbol="RB.SHF", date="2024-04-30")` 实测成功
- `index_daily(symbol="NHCI.NH", date="2024-04-30")` 实测成功
- `trade_cal(exchange="GFEX", 2024-04-29 ~ 2024-04-30)` 实测成功
- 通过 `DataUpdater` 和数据库写入后，以上数据已能从库中读回确认

## 参考项目说明

并列项目 `../futures_nexus` 仅作为业务逻辑参考，当前只借用了以下思路：

- 主力月份配置
- 期限结构计算思路
- 跨期价差计算思路

未复用部分：

- SQLite 存储
- DataWorks 封装
- 原下载脚本框架

## 已知限制

- 期货分钟线真实下载仍依赖本地 `xtquant_helper` 背后的 Windows XtQuant 客户端状态
- Tushare 分钟线、小时线、tick 权限不纳入当前 V1
- `futures_inventory_99` 只能按品种名称拉全历史，不能指定时间范围
- `get_receipt` 长时间窗口容易失败，当前实现已固定按 5 天窗口切片重试
