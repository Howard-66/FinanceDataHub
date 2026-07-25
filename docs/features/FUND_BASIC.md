# 公募基金列表（`fund_basic`）

FinanceDataHub 支持同步和查询 Tushare `fund_basic` 公募基金列表。该接口包含场内基金（`E`）和场外基金（`O`），需要 Tushare 账号具备至少 2000 积分权限。

## 同步

```bash
# 同步全部场内、场外基金
fdh-cli update --dataset fund_basic

# 只同步场内或场外基金
fdh-cli update --dataset fund_basic --symbols E
fdh-cli update --dataset fund_basic --symbols O
```

`fund_basic` 为非时间序列目录，不接受 `--trade-date`、`--start-date` 或 `--end-date`。调度任务 `fund_basic_update` 会在每周一 07:10（Asia/Shanghai）执行完整同步。

Tushare 单次最多返回 15,000 条。本项目对每个基金市场从 `offset=0` 开始；若某页恰好返回 15,000 条，会将 offset 增加 15,000 并继续请求，直到某页少于 15,000 条。每个市场均独立分页，因此不会遗漏场外基金。

## SDK 查询

```python
funds = fdh.get_fund_basic(market="E", status="L")

funds_async = await fdh.get_fund_basic_async(
    management="华夏基金",
    fund_type="股票型",
)
```

筛选参数包括 `ts_code`、`market`（`E` / `O`）、`status`（`D` / `I` / `L`）、`fund_type` 和 `management`。SDK 从本地 `fund_basic` 表读取已同步的数据。

## 存储字段

表 `fund_basic` 保留 Tushare 接口的全部输出字段：

`ts_code`、`name`、`management`、`custodian`、`fund_type`、`found_date`、`due_date`、`list_date`、`issue_date`、`delist_date`、`issue_amount`、`m_fee`、`c_fee`、`duration_year`、`p_value`、`min_amount`、`exp_return`、`benchmark`、`status`、`invest_type`、`type`、`trustee`、`purc_startdate`、`redm_startdate`、`market`。

`ts_code` 是主键；重复同步时会按该字段更新现有记录。新部署由初始化脚本创建表，已有数据库可执行迁移 [031_create_fund_basic.sql](../../sql/migrations/031_create_fund_basic.sql)。
