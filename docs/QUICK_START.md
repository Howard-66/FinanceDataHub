# FinanceDataHub 快速开始指南

欢迎使用 FinanceDataHub！这是一个基于现代 Python 技术的综合性金融数据服务中心。

## 📋 系统要求

- Python 3.11+
- Docker & Docker Compose
- uv 包管理器（可选，用于依赖管理）

## 🚀 快速开始

### 1. 环境准备

#### 启动依赖服务
```bash
# 启动PostgreSQL和Redis
docker-compose up -d

# 检查服务状态
docker-compose ps
```

#### 配置环境变量
创建 `.env` 文件：
```bash
# 数据库配置
DATABASE_URL=postgresql://trading_nexus:trading.nexus.data@localhost:5432/trading_nexus_db

# Redis配置
REDIS_URL=redis://localhost:6379/0

# Tushare Token（从 https://tushare.pro/ 获取）
TUSHARE_TOKEN=7c21668102a23bf1ea79451f22f6801e7365a7a15db5348b9f84f16c

# XTQuant API地址（可选，如果使用XTQuant）
XTQUANT_API_URL=http://localhost:8100
```

#### 配置数据源路由
复制并编辑配置文件：
```bash
cp sources.yml.example sources.yml
# 根据需要修改sources.yml配置
```

### 2. 初始化数据库

使用CLI命令自动初始化数据库：
```bash
# 初始化数据库（创建所有必需的表和扩展）
fdh-cli init

# 或显示详细信息
fdh-cli init --verbose
```

这将自动：
- 创建TimescaleDB扩展
- 创建所有数据表（asset_basic, symbol_daily, symbol_minute等）
- 创建索引和约束
- 创建视图和辅助函数

⚠️ **注意**: 首次使用系统前必须执行此命令！

### 3. 安装依赖

#### 使用 uv（推荐）

```bash
# 安装 uv（如果尚未安装）
curl -LsSf https://astral.sh/uv/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"

# 同步依赖
uv sync

# 激活虚拟环境
source .venv/bin/activate
```

#### 使用 pip

```bash
# 安装核心依赖
pip install -e .

# 安装开发依赖（可选）
pip install -e ".[dev]"
```

### 4. 验证安装

```bash
# 测试 CLI 命令
fdh-cli --help

# 查看系统状态
fdh-cli status

# 查看当前配置
fdh-cli config
```

## 📖 基本使用

### 查看帮助
```bash
# CLI帮助
fdh-cli --help

# 具体命令帮助
fdh-cli init --help
fdh-cli update --help
fdh-cli status --help
```

### 更新数据

#### 1. 更新股票基本信息
```bash
# 获取所有股票列表
fdh-cli update --frequency basic

# 仅获取指定市场的股票
# （注：当前版本会获取全部，可通过修改updater.py支持market参数）
```

**注意：** 运行此命令需要：
- 正确配置 `.env` 文件中的 `DATABASE_URL`
- 正确配置 `TUSHARE_TOKEN`
- PostgreSQL和Redis服务正在运行
- 正确配置 `sources.yml` 文件


#### 2. 更新日线数据
```bash
# 增量更新最近30天数据（自动检测最新数据）
fdh-cli update --frequency daily

# 更新指定股票
fdh-cli update --frequency daily --symbols 600519.SH,000858.SZ

# 获取前复权数据
fdh-cli update --frequency daily --symbols 600519.SH --adj qfq

# 指定日期范围
fdh-cli update --frequency daily --symbols 600519.SH \
    --start-date 2024-01-01 --end-date 2024-12-31
```

#### 3. 更新分钟数据
```bash
# 1分钟数据（最近1天）
fdh-cli update --frequency minute_1 --symbols 600519.SH

# 5分钟数据
fdh-cli update --frequency minute_5 --symbols 600519.SH

# 查看详细日志
fdh-cli update --frequency minute_1 --symbols 600519.SH --verbose
```

#### 4. 更新每日指标
```bash
# 获取PE、PB等指标数据
fdh-cli update --frequency daily_basic --symbols 600519.SH
```

### 查看状态

#### 系统状态
```bash
# 基础状态
fdh-cli status

# 详细状态
fdh-cli status --verbose
```

#### 配置信息
```bash
# 查看当前配置
fdh-cli config

# 重新加载配置
fdh-cli config --reload
```

## 🎯 使用场景示例

### 场景1：获取股票基本信息
```bash
# Step 1: 获取所有股票列表
fdh-cli update --frequency basic

# Step 2: 查看数据库中的股票数量
psql postgresql://fdh_user:fdh_password@localhost:5432/financedatahub \
    -c "SELECT COUNT(*) FROM asset_basic WHERE list_status='L';"
```

### 场景2：定期更新日线数据
```bash
# 创建定时任务（每天收盘后执行）
# 使用cron或其他调度器
0 18 * * 1-5 fdh-cli update --frequency daily --verbose

# 或手动执行
fdh-cli update --frequency daily
```

### 场景3：批量获取多只股票数据
```bash
# 更新多个股票（逗号分隔）
fdh-cli update --frequency daily \
    --symbols 600519.SH,000858.SZ,000001.SZ,600036.SH \
    --start-date 2024-01-01 --end-date 2024-12-31
```

### 场景4：获取分钟数据进行分析
```bash
# 获取1分钟数据
fdh-cli update --frequency minute_1 \
    --symbols 600519.SH \
    --start-date 2024-12-01 \
    --end-date 2024-12-02

# 查看数据量
psql postgresql://fdh_user:fdh_password@localhost:5432/financedatahub \
    -c "SELECT symbol, COUNT(*) FROM symbol_minute WHERE symbol='600519.SH' GROUP BY symbol;"
```

## 🔍 故障排除

### 问题1：Tushare Token无效
```
错误：ProviderAuthError: Invalid Tushare token
解决：
1. 检查.env文件中的TUSHARE_TOKEN是否正确
2. 登录 https://tushare.pro/ 获取新的token
3. 执行 fdh-cli config --reload 重新加载配置
```

### 问题2：无法连接到PostgreSQL
```
错误：数据库连接失败
解决：
1. 检查docker-compose是否运行：docker-compose ps
2. 检查连接信息：DATABASE_URL
3. 检查防火墙和网络设置
```

### 问题3：XTQuant连接失败
```
错误：ProviderConnectionError: Failed to connect to xtquant_helper
解决：
1. 检查xtquant_helper服务是否运行
2. 检查XTQUANT_API_URL配置
3. 如果不使用XTQuant，可以禁用sources.yml中的xtquant配置
```

### 问题4：路由失败
```
错误：ProviderError: All providers failed
解决：
1. 检查sources.yml配置是否正确
2. 检查网络连接
3. 查看日志：fdh-cli update --verbose
```

## 📊 数据验证

### 查看数据完整性
```sql
-- 查看各表数据量
SELECT
    'asset_basic' as table_name, COUNT(*) as count FROM asset_basic
UNION ALL
SELECT
    'symbol_daily' as table_name, COUNT(*) as count FROM symbol_daily
UNION ALL
SELECT
    'symbol_minute' as table_name, COUNT(*) as count FROM symbol_minute
UNION ALL
SELECT
    'daily_basic' as table_name, COUNT(*) as count FROM daily_basic;
```

### 检查最新数据
```sql
-- 查看每只股票的最新交易日期
SELECT
    symbol,
    MAX(time) as latest_date,
    COUNT(*) as total_records
FROM symbol_daily
GROUP BY symbol
ORDER BY latest_date DESC;
```

### 查看数据样本
```sql
-- 查看最新数据样本
SELECT *
FROM symbol_daily
WHERE symbol = '600519.SH'
ORDER BY time DESC
LIMIT 5;
```

## ⚡ 性能优化建议

### 1. 批量更新
- 尽量使用 `--symbols` 批量更新，而不是单个股票
- 避免频繁的少量更新

### 2. 增量更新
- 系统会自动检测最新数据，只获取缺失的数据
- 定期执行更新比大量一次性更新更高效

### 3. 索引优化
- 已在关键列上创建索引（symbol, time）
- 避免在WHERE子句中对time列使用函数

### 4. 并发控制
- Tushare限制200次/分钟调用，已实现自动限频
- XTQuant调用较慢，建议分批更新

## 🐍 Python API

```python
from finance_data_hub.config import get_settings

# 获取配置
settings = get_settings()

# 使用配置
db_url = settings.database.url
redis_url = settings.redis.url
```

## 🧪 运行测试

```bash
# 运行所有测试
pytest

# 运行特定测试
pytest tests/unit/test_config.py

# 生成覆盖率报告
pytest --cov=finance_data_hub
```

## 📁 项目结构

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

## 🔧 开发指南

### 代码风格

项目使用以下工具确保代码质量：

```bash
# 代码格式化
black .

# 导入排序
isort .

# 代码检查
flake8

# 类型检查
mypy finance_data_hub
```

### 添加新功能

1. 在相应的模块中添加代码
2. 编写单元测试
3. 更新文档
4. 运行所有测试

### 提交代码

使用 Conventional Commits 格式：

```bash
git commit -m "feat(config): add new database connection option"
```

## 🔄 下一步

### Phase 3 即将实现
1. **数据访问SDK** - Python SDK for数据查询
2. **完整ETL** - PostgreSQL → Parquet + DuckDB
3. **实时数据** - WebSocket流式数据
4. **Web UI** - 图形化数据展示

### 贡献代码
- 报告Bug：创建Issue
- 提出新功能：创建Feature Request
- 提交代码：Fork & Pull Request

## 📚 更多资源

- [项目文档](./README.md) - 项目概览和完整功能列表
- [最终交付报告](./FINAL_SUMMARY.md) - Phase 2完整交付文档，包含Bug修复记录、功能验证清单等
- [数据源配置](./sources.yml.example) - 数据源路由配置示例

## ❓ 常见问题

**Q: 如何连接数据库？**

A: 默认连接信息：
- Host: localhost
- Port: 5432
- Database: trading_nexus_db
- User: trading_nexus
- Password: trading.nexus.data

**Q: uv 同步依赖失败怎么办？**

A: 尝试清理并重新同步：
```bash
rm -rf .venv uv.lock
uv sync
```

**Q: 如何查看详细的日志？**

A: 修改 `.env` 文件中的 `LOG_LEVEL` 为 `DEBUG`：
```bash
LOG_LEVEL=DEBUG
```

**Q: 可以同时使用Tushare和XTQuant吗？**
A: 是的，系统会自动根据路由策略选择数据源，并支持故障转移。

**Q: 数据更新频率建议？**
A: 日线数据建议每日收盘后更新，分钟数据建议盘中或盘后更新。

**Q: 支持哪些复权类型？**
A: 支持 None（不复权）、qfq（前复权）、hfq（后复权）。

**Q: 如何备份数据？**
A: 使用PostgreSQL的pg_dump工具，或配置定时备份脚本。

---

**开始使用：**
```bash
# 1. 初始化数据库
fdh-cli init

# 2. 更新股票基本信息
fdh-cli update --frequency basic
```
