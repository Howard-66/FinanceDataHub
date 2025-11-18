## Why

当前的增量更新逻辑过于简单，仅根据最后一条记录的时间戳获取增量数据。这会导致几个问题：

1. **盘中数据覆盖问题**：数据库中最后一条记录可能是盘中请求的未完成K线数据，并非该时间段的真实收盘价，需要智能判断是否应该覆盖
2. **新symbol初始化问题**：当symbol在数据库中不存在时，没有智能的日期范围确定策略
3. **一致性缺失**：增量更新逻辑目前仅在CLI层面简单描述，没有形成统一的数据提供者层面的智能策略

## What Changes

### 核心改进

1. **数据库查询逻辑增强**
   - 新增 `get_latest_record()` 方法获取数据库中指定symbol的最新记录
   - 新增 `should_overwrite_latest_record()` 方法判断是否需要覆盖最后的盘中数据
   - 对日线数据：如果最后记录时间是今天且当前时间在交易时间后，使用全量覆盖策略

2. **智能日期范围确定**
   - 对于指定symbol：
     - 当传入start_date/end_date时，使用指定的时间范围
     - 当start_date/end_date为空时，下载该股票的全量数据
     - 基于数据库最后记录时间和当前时间进行增量更新
   - 对于未指定symbol的全资产更新：
     - Tushare：使用 `trade_date` 参数，按日期下载全部股票数据
     - 其他provider：对 asset_basic 中所有资产执行全量更新
   - 不再需要从 asset_basic 获取 list_date，因为provider API本身支持全量下载

3. **CLI参数名称优化**
   - `--frequency` 参数命名不够准确，因为它涵盖了多种数据类型：
     - 时间周期类：daily, minute_1, minute_5, tick
     - 数据类型类：daily_basic, financial_indicator, balance_sheet, income_statement
   - 建议更改为更准确的参数名：
     - `--dataset` 或 `--data-type`：更准确地描述数据类型
     - `--category`：数据类别
   - 保持向后兼容：支持旧参数名但添加新参数名

4. **统一增量更新策略**
   - 将增量更新逻辑提升到数据提供者基类层面
   - 新增 `get_incremental_data()` 方法，适配所有时间序列数据（daily、minute、tick等）
   - 智能处理不同频率数据的日期范围计算
   - 支持时间序列数据和非时间序列数据的增量更新

5. **错误处理和容错机制**
   - 当增量数据获取失败时，自动回退到全量更新
   - 新增数据验证逻辑，确保增量数据的完整性

### 涉及的规格

- **cli-update**: 修改现有增量更新场景，增强覆盖判断逻辑
- **data-providers**: 新增增量数据获取抽象方法，实现智能日期范围确定

## Impact

### 受影响的规格
- `specs/cli-update/spec.md` - 修改现有增量更新逻辑
- `specs/data-providers/spec.md` - 新增增量数据获取能力

### 受影响的代码
- `finance_data_hub/providers/base.py` - 新增增量更新抽象方法
- `finance_data_hub/providers/tushare.py` - 实现增量更新逻辑
- `finance_data_hub/providers/xtquant.py` - 实现增量更新逻辑
- CLI update命令实现 - 集成新的增量更新策略

### 预期收益
- **数据质量提升**：避免盘中未完成数据的覆盖问题
- **更新效率提高**：智能的增量更新减少不必要的全量请求
- **开发者体验改善**：统一的增量更新API，降低使用复杂度
- **数据一致性**：所有时间序列数据采用一致的更新策略

### 风险
- 新逻辑引入的复杂度增加，需要充分测试
- 对现有代码的修改可能影响已有功能

## Breaking Changes
- 无破坏性API变更
- 现有CLI命令行为保持兼容，新增智能判断逻辑
