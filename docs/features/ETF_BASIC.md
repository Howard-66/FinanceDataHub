# ETF 基础信息（`etf_basic`）

FinanceDataHub 支持同步和查询 Tushare [`etf_basic`](https://tushare.pro/document/2?doc_id=385) 国内 ETF 基础信息，数据包含境内 ETF 与 QDII ETF。

## 同步

```bash
# 分页同步全部 ETF
fdh-cli update --dataset etf_basic
fdh-cli update --dataset etf_basic --symbols all

# 同步单个 ETF
fdh-cli update --dataset etf_basic --symbols 510300.SH
```

`etf_basic` 是非时间序列目录，不支持 `--trade-date`、`--start-date` 或 `--end-date`。单次请求最多返回 5,000 条；返回数量达到上限时，Provider 会增加 `offset` 并继续请求，直到某页少于 5,000 条。

调度任务 `etf_basic_update` 默认在每周一 07:30（Asia/Shanghai）执行全量刷新。

## SDK 查询

```python
etfs = fdh.get_etf_basic(
    exchange="SH",
    list_status="L",
    etf_type="境内",
)

etfs_async = await fdh.get_etf_basic_async(
    index_code="000300.SH",
    mgr_name="华夏基金",
)
```

SDK 从本地 `etf_basic` 表读取数据，支持通过 `ts_code`、`index_code`、`list_date`、`list_status`、`exchange`、`mgr_name` 和 `etf_type` 筛选。

## 存储字段

表 `etf_basic` 保存接口全部 14 个输出字段：

`ts_code`、`csname`、`extname`、`cname`、`index_code`、`index_name`、`setup_date`、`list_date`、`list_status`、`exchange`、`mgr_name`、`custod_name`、`mgt_fee`、`etf_type`。

`ts_code` 为主键，重复同步时执行 upsert。新部署由数据库初始化脚本创建表；已有数据库需要手动执行迁移 [035_create_etf_basic.sql](../../sql/migrations/035_create_etf_basic.sql)。
