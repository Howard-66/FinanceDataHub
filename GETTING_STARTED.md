# FinanceDataHub - 快速开始指南

欢迎使用 FinanceDataHub！这是一个基于现代 Python 技术的综合性金融数据服务中心。

## 📋 系统要求

- Python 3.11+
- Docker & Docker Compose
- uv 包管理器（可选，用于依赖管理）

## 🚀 快速开始

### 1. 启动 Docker 服务

```bash
# 启动 PostgreSQL 和 Redis 服务
docker-compose up -d

# 验证服务状态
docker-compose ps
```

预期输出：
```
NAME                      IMAGE                               SERVICE       STATUS                       PORTS
trading_nexus_container   timescale/timescaledb:latest-pg17   timescaledb   Up About an hour (healthy)   0.0.0.0:5432->5432/tcp
trading_nexus_redis       redis:7-alpine                      redis         Up About an hour (healthy)   0.0.0.0:6379->6379/tcp
```

### 2. 设置环境变量

```bash
# 复制环境变量模板
cp .env.example .env

# 编辑 .env 文件（可选，使用默认值即可）
# vim .env
```

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

## 📖 使用指南

### CLI 命令详解

#### `fdh-cli update`

从数据源更新数据到数据库。

```bash
# 基础使用
fdh-cli update

# 指定资产类别和数据频率
fdh-cli update --asset-class stock --frequency daily

# 指定特定股票代码
fdh-cli update --symbols 600519.SH,000858.SZ

# 显示详细输出
fdh-cli update --verbose
```

#### `fdh-cli etl`

执行 ETL 流程（Phase 3 中实现）。

```bash
# 基础 ETL
fdh-cli etl --from-date 2024-01-01

# 指定日期范围
fdh-cli etl --from-date 2024-01-01 --to-date 2024-12-31

# 试运行（不执行实际 ETL）
fdh-cli etl --from-date 2024-01-01 --dry-run
```

#### `fdh-cli status`

查看系统状态和数据完整性。

```bash
# 基础状态检查
fdh-cli status

# 详细状态
fdh-cli status --verbose

# JSON 格式输出
fdh-cli status --format json
```

#### `fdh-cli config`

查看和管理配置。

```bash
# 显示当前配置
fdh-cli config

# 重新加载配置
fdh-cli config --reload
```

### Python API

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
├── cli/                    # CLI 模块
│   └── main.py            # CLI 主入口
├── config.py              # 配置管理
├── providers/             # 数据提供者（Phase 2）
├── storage/               # 存储模块（Phase 3）
└── utils/                 # 工具函数

tests/
├── unit/                  # 单元测试
│   ├── test_config.py
│   └── test_cli.py
└── integration/           # 集成测试（后续添加）

.env.example              # 环境变量模板
pyproject.toml           # 项目配置
uv.lock                 # 依赖锁定文件
docker-compose.yml       # Docker 编排配置
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

## ❓ 常见问题

### Q: 如何连接数据库？

A: 默认连接信息：
- Host: localhost
- Port: 5432
- Database: trading_nexus_db
- User: trading_nexus
- Password: trading.nexus.data

### Q: uv 同步依赖失败怎么办？

A: 尝试清理并重新同步：
```bash
rm -rf .venv uv.lock
uv sync
```

### Q: 如何查看详细的日志？

A: 修改 `.env` 文件中的 `LOG_LEVEL` 为 `DEBUG`：
```bash
LOG_LEVEL=DEBUG
```

## 📚 下一步

Phase 1 完成！现在可以继续：

- [Phase 2: 核心批处理流程](./README.md#实施计划)
- [Phase 3: 数据访问与查询](./README.md#实施计划)
- [Phase 4: 流式处理与高级特性](./README.md#实施计划)

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 许可证

MIT License
