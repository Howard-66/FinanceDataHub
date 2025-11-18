## Implementation Tasks

### Phase 1: Core Incremental Update Logic
- [x] 1.1 新增 `get_latest_record()` 抽象方法到 BaseDataProvider
- [x] 1.2 新增 `should_overwrite_latest_record()` 抽象方法到 BaseDataProvider
- [x] 1.3 新增 `get_incremental_data()` 抽象方法到 BaseDataProvider
- [x] 1.4 实现 `calculate_date_range()` 工具方法用于智能日期计算

### Phase 2: Data Provider Implementations
- [x] 2.1 在 TushareProvider 中实现 `get_latest_record()` 方法
- [x] 2.2 在 TushareProvider 中实现 `should_overwrite_latest_record()` 方法
- [x] 2.3 在 TushareProvider 中实现 `get_incremental_data()` 方法
- [x] 2.4 在 XTQuantProvider 中实现相同的增量更新方法

### Phase 3: CLI Update Enhancement
- [x] 3.1 修改 `fdh-cli update` 命令，集成新的增量更新逻辑
- [x] 3.2 新增 `--dataset` 参数替代 `--frequency`，支持多种数据类型
- [x] 3.3 保持 `--frequency` 参数向后兼容，添加废弃警告
- [x] 3.4 新增 `--smart-incremental` 标志启用智能增量更新
- [x] 3.5 更新增量更新场景的验证逻辑，支持时间序列和非时间序列数据
- [x] 3.6 新增日志记录，清晰显示增量更新决策过程

### Phase 4: Testing
- [ ] 4.1 编写 BaseDataProvider 增量更新抽象方法测试
- [ ] 4.2 编写 TushareProvider 增量更新功能测试
- [ ] 4.3 编写 XTQuantProvider 增量更新功能测试
- [ ] 4.4 编写 CLI 增量更新集成测试
- [ ] 4.5 测试盘中数据覆盖判断逻辑
- [ ] 4.6 测试新symbol的智能日期范围确定
- [ ] 4.7 测试 `--dataset` 参数和 `--frequency` 向后兼容性

### Phase 5: Documentation
- [ ] 5.1 更新 API 文档，说明新的增量更新方法
- [ ] 5.2 更新 CLI 使用文档，新增智能增量更新示例和 --dataset 参数说明
- [ ] 5.3 创建增量更新策略技术文档
- [ ] 5.4 添加参数迁移指南（从 --frequency 到 --dataset）

### Phase 6: Validation
- [ ] 6.1 运行完整的增量更新测试套件
- [ ] 6.2 验证所有场景下的数据完整性
- [ ] 6.3 性能测试确保增量更新比全量更新更高效
- [ ] 6.4 运行 `openspec validate optimize-incremental-update-logic --strict`
