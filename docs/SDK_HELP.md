# 使用 FinanceDataHub 的 SDK：

## 方案一：使用 uv 添加路径依赖（推荐）
在 ValueInvesting 的 pyproject.toml 中添加 FinanceDataHub 作为本地依赖：

``` toml
[project]
name = "valueinvesting"
# ... 其他配置

[tool.uv]
dev-dependencies = [
    "finance-data-hub @ git+file:///Volumes/Repository/Projects/TradingNexus/FinanceDataHub",
    # 或者使用相对路径：
    # "finance-data-hub @ file:///Volumes/Repository/Projects/TradingNexus/FinanceDataHub",
]
```
然后运行：
``` bash
cd /Volumes/Repository/Projects/TradingNexus/ValueInvesting
uv sync
```

## 方案二：设置 PYTHONPATH
在 ValueInvesting 的 .env 或启动脚本中设置：

export PYTHONPATH="/Volumes/Repository/Projects/TradingNexus/FinanceDataHub:$PYTHONPATH"
配置共享
由于两个项目共用同一个数据库，需要确保 ValueInvesting 能访问 FinanceDataHub 的配置：

共享 .env 文件：将 FinanceDataHub 的 .env 配置复制到 ValueInvesting，或创建符号链接
共享 sources.yml：同上
使用示例
在 ValueInvesting 的代码中使用：

```python
import sys
sys.path.insert(0, '/Volumes/Repository/Projects/TradingNexus/FinanceDataHub')

from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

# 初始化
settings = get_settings()
fdh = FinanceDataHub(settings=settings)

# 获取股票数据
daily_data = fdh.get_daily(['600519.SH'], '2024-01-01', '2024-12-31')
print(f"日线数据: {len(daily_data)} 条记录")

# 获取财务指标
fina_data = fdh.get_fina_indicator('600519.SH', '2020-03-31', '2024-12-31')
```