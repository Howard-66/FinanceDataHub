# FinanceDataHub 快速开始指南

## 🚀 5分钟快速上手

本指南将帮助您在5分钟内快速启动 FinanceDataHub 并获取第一批数据。

---

## 前提条件

- Python 3.10 或更高版本
- Docker 和 Docker Compose
- Tushare Pro Token (免费注册: https://tushare.pro/)

---

## 步骤 1: 启动服务

```bash
# 克隆项目
git clone <repository-url>
cd FinanceDataHub

# 启动 PostgreSQL 和 Redis
docker-compose up -d
```

**验证服务**:
```bash
docker ps
# 应该看到 postgres 和 redis 容器在运行
```

---

## 步骤 2: 安装依赖

```bash
# 使用 uv 安装依赖 (推荐)
uv sync

# 或者使用 pip
pip install -e .
```

---

## 步骤 3: 配置环境变量

```bash
# 复制示例配置
cp .env.example .env

# 编辑配置文件
# 至少需要设置:
# - TUSHARE_TOKEN=your_token_here
# - DATABASE_URL=postgresql://fdh:fdh123@localhost:5432/financedatahub
```

**获取 Tushare Token**:
1. 注册 https://tushare.pro/ 账户
2. 登录后进入 "个人中心" -> "接口 Token"
3. 复制 token 到 `.env` 文件

---

## 步骤 4: 初始化数据库

```bash
# 初始化数据库表结构
psql "$DATABASE_URL" -f sql/init/001_create_extensions.sql
psql "$DATABASE_URL" -f sql/init/002_create_tables.sql
psql "$DATABASE_URL" -f sql/init/003_create_hypertables.sql
psql "$DATABASE_URL" -f sql/init/004_create_adj_factor.sql
```

---

## 步骤 5: 获取第一批数据

### 5.1 更新股票基本信息 (必须)

```bash
fdh-cli update --dataset basic
```

这将获取所有股票的基本信息，耗时约1分钟。

### 5.1.1 更新港股股票列表 (HK)

港股股票列表、日线和分钟线依赖 `xtquant_helper`，开始前请确认：

```bash
# .env 中至少需要有 XTQUANT_API_URL
XTQUANT_API_URL=http://<your-xtquant-helper-host>:8100
```

然后执行：

```bash
# 更新港股股票列表
fdh-cli update --dataset basic --market HK

# 同时更新 A 股和港股股票列表
fdh-cli update --dataset basic --market ALL
```

说明：
- 港股 `basic` 通过 XtQuant 板块 `香港联交所股票` 获取
- v1 版本港股基础信息以代码列表为主，名称缺失时会使用 symbol 占位

### 5.2 智能下载日线数据 (推荐)

```bash
# 智能模式（默认）- 自动检测数据库状态
fdh-cli update --dataset daily
```

**智能下载逻辑**:
- 第一次运行: 获取全部历史数据 (约5000只股票，需要15-30分钟)
- 后续运行: 只获取增量数据 (仅新交易日，约10秒)

### 5.2.1 港股日线更新

```bash
# 智能增量更新全部港股日线
fdh-cli update --dataset daily --market HK

# 强制补历史区间
fdh-cli update --dataset daily --market HK --force \
  --start-date 2024-01-01 --end-date 2024-12-31

# 只更新指定港股
fdh-cli update --dataset daily --market HK --symbols 00700.HK,00005.HK

# 指定交易日更新全部港股日线
fdh-cli update --dataset daily --market HK --trade-date 2024-11-18
```

说明：
- 港股日线固定从 XtQuant 获取，避免误走 Tushare
- 落库保存的是原始未复权日线；前/后复权依赖 `adj_factor`

### 5.3 指定日期范围 (可选)

```bash
# 获取特定日期范围的数据
fdh-cli update --dataset daily --start-date 2024-01-01 --end-date 2024-12-31
```

### 5.4 更新指定股票 (可选)

```bash
# 只更新特定股票 (节省时间)
fdh-cli update --dataset daily --symbols 600519.SH,000858.SZ
```

### 5.5 更新港股分钟线 (可选)

```bash
# 1分钟线
fdh-cli update --dataset minute_1 --market HK --symbols 00700.HK

# 5分钟线
fdh-cli update --dataset minute_5 --market HK --symbols 00700.HK

# 15分钟 / 30分钟 / 60分钟线
fdh-cli update --dataset minute_15 --market HK --symbols 00700.HK
fdh-cli update --dataset minute_30 --market HK --symbols 00700.HK
fdh-cli update --dataset minute_60 --market HK --symbols 00700.HK

# 不传 symbols 时，CLI 会从港股股票池中按默认 limit 抽样更新
fdh-cli update --dataset minute_1 --market HK
```

### 5.6 更新港股复权因子 (推荐与日线配套执行)

```bash
# 智能增量更新全部港股复权因子
fdh-cli update --dataset adj_factor --market HK

# 指定港股或日期范围
fdh-cli update --dataset adj_factor --market HK --symbols 00700.HK
fdh-cli update --dataset adj_factor --market HK --force \
  --start-date 2024-01-01 --end-date 2024-12-31

# 指定交易日
fdh-cli update --dataset adj_factor --market HK --trade-date 2024-11-18
```

说明：
- 港股 `adj_factor` 由 XtQuant 的未复权日线与 `back_ratio/back` 日线推导
- 当前不支持港股 `daily_basic`

### 5.7 港股技术指标预处理

在港股 `daily` 和 `adj_factor` 更新完成后，可以直接运行技术指标预处理：

```bash
# 处理全部港股（日线 + 周线 + 月线）
fdh-cli preprocess run --all --category technical --market HK

# 处理指定港股
fdh-cli preprocess run --category technical --market HK \
  --symbols 00700.HK,00005.HK

# 指定频率与复权方式
fdh-cli preprocess run --all --category technical --market HK \
  --freq daily,weekly,monthly --adjust qfq

# 强制全量重算
fdh-cli preprocess run --all --category technical --market HK --force
```

说明：
- `preprocess run` 默认 `--market CN`
- `--all --market HK` 会自动使用港股股票池，不需要手动传 symbol 列表
- 港股当前仅支持 `technical` 预处理
- `fundamental`、`quarterly_fundamental`、`industry_valuation`、`all` 仍然仅适用于 A 股
- 如果通过 `schedules.yml` 调度，现有 A 股股票类任务建议显式写 `market: CN`，港股使用单独的 `hk_*` 任务

---

## 步骤 6: 查看数据状态

```bash
# 查看数据更新状态
fdh-cli status --verbose

# 查看帮助
fdh-cli --help
```

---

## 🎯 常见使用场景

### 场景 1: 日常数据维护 (每日收盘后)

```bash
# 获取最新交易日数据
fdh-cli update --dataset daily
```

### 场景 2: 补充历史数据

```bash
# 强制更新某日期范围
fdh-cli update --dataset daily --force --start-date 2024-01-01 --end-date 2024-12-31
```

### 场景 3: 批量更新交易日数据

```bash
# 批量获取指定交易日所有股票数据 (约2秒，5000+股票)
fdh-cli update --dataset daily --trade-date 2024-11-18
```

### 场景 4: 多数据类型更新

```bash
# 更新每日指标数据
fdh-cli update --dataset daily_basic

# 更新复权因子
fdh-cli update --dataset adj_factor

# 更新指数日线行情（项目支持指数）
fdh-cli update --dataset index_daily
fdh-cli update --dataset index_daily --symbols 000300.SH
```

### 场景 5: 港股全链路更新

```bash
# 1. 先刷新港股股票池
fdh-cli update --dataset basic --market HK

# 2. 再更新港股日线
fdh-cli update --dataset daily --market HK

# 3. 更新港股复权因子
fdh-cli update --dataset adj_factor --market HK

# 4. 按需更新分钟线
fdh-cli update --dataset minute_1 --market HK --symbols 00700.HK,00005.HK
```

### 场景 6: 港股行情更新 + 技术指标预处理

```bash
# 1. 更新港股股票池
fdh-cli update --dataset basic --market HK

# 2. 更新港股日线
fdh-cli update --dataset daily --market HK

# 3. 更新港股复权因子
fdh-cli update --dataset adj_factor --market HK

# 4. 预处理全部港股技术指标
fdh-cli preprocess run --all --category technical --market HK
```

---

## 📊 验证数据

使用 Python 直接查询数据库:

```python
import pandas as pd
from sqlalchemy import create_engine

# 连接数据库
engine = create_engine("postgresql://fdh:fdh123@localhost:5432/financedatahub")

# 查询数据
df = pd.read_sql("""
    SELECT * FROM symbol_daily
    WHERE symbol = '600519.SH'
    ORDER BY time DESC
    LIMIT 10
""", engine)

print(df)
```

---

## ⚡ 性能优化提示

1. **首次运行**: 建议先更新单只股票测试
   ```bash
   fdh-cli update --dataset daily --symbols 600519.SH
   ```

2. **智能下载**: 默认使用智能模式，避免重复获取

3. **批量更新**: 使用 `--trade-date` 批量更新单个交易日（股票 `daily/daily_basic/adj_factor` 等）
   `index_daily` 不支持 `--trade-date` 全指数单日批量模式

4. **并行处理**: 系统自动批量插入 (每批1000条)，无需手动处理

5. **港股预处理**: 处理全部港股技术指标时，优先使用 `fdh-cli preprocess run --all --category technical --market HK`

---

## 🔧 故障排除

### 问题: 连接数据库失败

**解决**:
```bash
# 检查数据库是否运行
docker ps | grep postgres

# 重启数据库
docker-compose restart postgres
```

### 问题: Tushare API 限频

**解决**: 系统自动重试，耐心等待即可。首次全量更新可能需要多次重试。

### 问题: 没有数据返回

**解决**:
1. 检查 Tushare token 是否正确
2. 检查股票代码是否正确 (格式: 600519.SH)
3. 检查日期是否为交易日 (排除周末和节假日)

### 问题: 港股列表或 K 线更新失败

**解决**:
1. 检查 `XTQUANT_API_URL` 是否指向可访问的 `xtquant_helper`
2. 检查 Windows 侧 QMT / XtQuant 是否已启动并有行情权限
3. 先执行 `fdh-cli update --dataset basic --market HK`，确认本地已有港股股票池
4. 如果是跑全部港股技术指标，请使用 `fdh-cli preprocess run --all --category technical --market HK`
5. 分钟线建议先用单只股票验证，例如 `00700.HK`

---

## 📚 更多资源

- **完整文档**: [README.md](./README.md)
- **CLI 命令**: `fdh-cli --help`
- **API 文档**: `finance_data_hub/` 目录
- **变更日志**: [CHANGELOG.md](./CHANGELOG.md)

---

## 🎉 下一步

恭喜！您已成功启动 FinanceDataHub。

接下来您可以:
1. 配置定时任务自动更新数据
2. 集成到您的量化分析流程
3. 使用 Jupyter 进行数据分析
4. 对接 Qlib、FinRL 等平台

---

**需要帮助?** 查看 [README.md](./README.md) 或提交 Issue。
