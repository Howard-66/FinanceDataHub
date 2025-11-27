## 1. SDK 基础数据查询方法
- [ ] 1.1 在 SDK 中添加 get_daily() 和 get_daily_async() 方法
- [ ] 1.2 在 SDK 中添加 get_minute() 和 get_minute_async() 方法（支持1/5/15/30/60分钟）
- [ ] 1.3 在 SDK 中添加 get_daily_basic() 和 get_daily_basic_async() 方法
- [ ] 1.4 在 SDK 中添加 get_adj_factor() 和 get_adj_factor_async() 方法
- [ ] 1.5 在 SDK 中添加 get_basic() 和 get_basic_async() 方法

## 2. 数据库操作查询方法
- [ ] 2.1 在 DataOperations 中实现日线数据查询（get_symbol_daily）
- [ ] 2.2 在 DataOperations 中实现分钟数据查询（get_symbol_minute）
- [ ] 2.3 在 DataOperations 中实现每日基本面查询（get_daily_basic）
- [ ] 2.4 在 DataOperations 中实现复权因子查询（已存在，需补充同步调用）
- [ ] 2.5 在 DataOperations 中实现股票基本信息查询（get_asset_basic）

## 3. SmartRouter 集成
- [ ] 3.1 将 SmartRouter 与 SDK 初始化集成
- [ ] 3.2 在所有查询方法中集成数据源选择逻辑
- [ ] 3.3 添加路由日志记录功能
- [ ] 3.4 更新 SDK 文档，记录智能路由使用指南

## 4. 数据类型支持
- [ ] 4.1 验证并测试所有数据类型查询的完整性
- [ ] 4.2 添加频率参数验证（minute_1, minute_5, minute_15, minute_30, minute_60）
- [ ] 4.3 添加复权类型参数支持（adj_factor查询）
- [ ] 4.4 添加股票基本信息缓存机制（非时间序列数据）

## 5. 测试与验证
- [ ] 5.1 为所有 SDK 查询方法编写集成测试
- [ ] 5.2 测试 SmartRouter 集成
- [ ] 5.3 验证所有数据类型查询的准确性和性能
- [ ] 5.4 测试异步查询性能

## 6. 文档
- [ ] 6.1 更新 SDK 使用示例，包含所有数据类型
- [ ] 6.2 记录智能路由配置和使用方法
- [ ] 6.3 为所有新方法添加 API 文档
