## Implementation Tasks

### Phase 1: Remove Incorrect Implementation
- [ ] 1.1 移除 `updater.py` 中错误的 `mode` 参数和默认30天/365天逻辑
- [ ] 1.2 移除 `cli/main.py` 中错误的 `--mode` 参数和智能增量模式逻辑
- [ ] 1.3 撤销之前对 `tushare.py` 中不正确的修改

### Phase 2: Implement Smart Download Logic
- [ ] 2.1 重新设计 `update_daily_data()` 方法，实现智能下载算法
- [ ] 2.2 实现查询数据库最新记录的逻辑
- [ ] 2.3 实现盘中数据覆盖判断逻辑
- [ ] 2.4 移除所有默认日期范围设置（30天、365天等）

### Phase 3: CLI Parameter Redesign
- [ ] 3.1 添加 `--force` 参数启用强制覆盖模式
- [ ] 3.2 添加 `--trade-date` 参数用于Tushare每日批量更新
- [ ] 3.3 移除 `--mode` 和 `--smart-incremental` 参数
- [ ] 3.4 实现更新策略矩阵，根据参数组合自动选择最优策略
- [ ] 3.5 更新参数优先级：`trade_date` > `symbols` + 日期范围

### Phase 4: Full Asset Update Optimization
- [ ] 4.1 实现未指定symbol时的全资产智能下载
- [ ] 4.2 实现Tushare的 `trade_date` 机制调用
- [ ] 4.3 实现按日期范围的定期批量更新逻辑
- [ ] 4.4 处理不同更新频率的优化策略

### Phase 5: Data Provider Support
- [ ] 5.1 更新 TushareProvider 支持空日期参数（全量下载）
- [ ] 5.2 更新 TushareProvider 支持 trade_date 参数
- [ ] 5.3 更新 XTQuantProvider 支持智能下载逻辑
- [ ] 5.4 验证各provider的API调用兼容性

### Phase 6: Testing
- [ ] 6.1 测试智能下载模式（新symbol全量，有symbol增量）
- [ ] 6.2 测试强制覆盖模式（--force参数）
- [ ] 6.3 测试盘中数据覆盖判断逻辑
- [ ] 6.4 测试全资产更新（trade_date机制）
- [ ] 6.5 测试更新策略矩阵（各种参数组合）
- [ ] 6.6 测试向后兼容性（--frequency参数）
- [ ] 6.7 测试不同数据类型的兼容性（daily, minute, daily_basic等）

### Phase 7: Documentation
- [ ] 7.1 更新API文档，说明智能下载和强制覆盖两种模式
- [ ] 7.2 更新CLI使用文档，添加新参数说明和使用示例
- [ ] 7.3 创建更新策略技术文档，说明策略矩阵
- [ ] 7.4 添加参数迁移指南

### Phase 8: Validation
- [ ] 8.1 运行完整的更新测试套件
- [ ] 8.2 验证所有场景下的数据完整性
- [ ] 8.3 性能测试确保智能下载的高效性
- [ ] 8.4 运行 `openspec validate optimize-incremental-update-logic --strict`
