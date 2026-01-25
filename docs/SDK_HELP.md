# 使用 FinanceDataHub 的 SDK：

## 添加FinanceDataHub依赖
在 ValueInvesting 的 pyproject.toml 中添加 FinanceDataHub 作为本地依赖：

``` toml
[project]
name = "valueinvesting"
# ... 其他配置

[tool.uv]
dev-dependencies = [
    # 使用相对路径：
    # "finance-data-hub @ file:///Volumes/Repository/Projects/TradingNexus/FinanceDataHub",
]
```
然后运行：
``` bash
cd /Volumes/Repository/Projects/TradingNexus/ValueInvesting
uv sync
```

## 复制配置文件内容
由于两个项目共用同一个数据库，需要确保 ValueInvesting 能访问 FinanceDataHub 的配置：

将 FinanceDataHub 的 .env 配置复制到 ValueInvesting
复制 sources.yml 到ValueInvesting

或创建符号链接：
``` bash
cd TradingNexus/ValueInvesting
ln -s ../FinanceDataHub/.env .env
ln -s ../FinanceDataHub/sources.yml sources.yml
```

使用示例
在 ValueInvesting 的代码中使用：

```python
from finance_data_hub import FinanceDataHub
from finance_data_hub.config import get_settings

# 初始化
settings = get_settings()
fdh = FinanceDataHub(
    settings=settings,
    backend="postgresql",
    router_config_path="sources.yml"  # 可选
)
await fdh.initialize()

fina_data = await fdh.get_fina_indicator_async('600519.SH')
fina_data

await fdh.close()
```