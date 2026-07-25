# 公募基金管理人和基金经理

FinanceDataHub 支持同步并查询 Tushare 的 `fund_company` 与 `fund_manager` 接口。

- `fund_company`：公募基金管理人目录，需要至少 1500 Tushare 积分，一次返回全部数据。
- `fund_manager`：基金经理任职和简历，需要至少 500 积分；单次最多 5,000 条，系统在返回记录数等于 5,000 时自动用 `offset` 继续分页。2,000 积分及以上可提高访问频次。

## 同步

```bash
# 全量同步基金管理人
fdh-cli update --dataset fund_company

# 全量分页同步基金经理
fdh-cli update --dataset fund_manager

# 同步指定基金的经理；支持逗号分隔多个基金代码
fdh-cli update --dataset fund_manager --symbols 150018.SZ,150008.SZ

# 按经理公告日同步
fdh-cli update --dataset fund_manager --trade-date 2010-05-08
```

`fund_company` 是目录数据，不接受日期或代码筛选。`fund_manager` 可按基金代码和公告日缩小接口范围，不支持日期区间。

## SDK 查询

```python
from finance_data_hub.sdk import FinanceDataHub

fdh = FinanceDataHub()

companies = fdh.get_fund_company(province="北京市")
managers = fdh.get_fund_manager(ts_code="150018.SZ")

# 异步查询
managers_async = await fdh.get_fund_manager_async(
    ts_code="150018.SZ", ann_date="20100508"
)
```

`get_fund_company()` 支持按 `name`、`province`、`city` 精确筛选；`get_fund_manager()` 支持按 `ts_code`、`ann_date`、`name` 精确筛选。

## 存储字段和主键

`fund_company` 保留接口的全部 18 个输出字段，以基金公司全称 `name` 作为主键。

`fund_manager` 保留接口的全部 10 个输出字段，以 `(ts_code, ann_date, name, begin_date)` 作为复合主键，从而保留同一基金经理的不同任职公告。

新部署由初始化脚本创建表；已有数据库请执行 [032_create_fund_company_and_fund_manager.sql](../../sql/migrations/032_create_fund_company_and_fund_manager.sql)。调度任务会在每周一 07:15 同步管理人目录、07:25 同步基金经理数据。
