-- 迁移脚本：为 symbol_minute 表添加 frequency 字段
-- 日期：2025-11-23
-- 目的：支持多种频率的分钟数据（1m, 5m, 15m, 30m, 60m）存储在同一张表中

-- ========================================
-- 步骤 1: 检查是否需要迁移
-- ========================================

DO $$
DECLARE
    column_exists BOOLEAN;
    has_data BOOLEAN;
    data_count BIGINT;
BEGIN
    -- 检查 frequency 字段是否已存在
    SELECT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'symbol_minute'
        AND column_name = 'frequency'
    ) INTO column_exists;

    IF column_exists THEN
        RAISE NOTICE '✓ frequency 字段已存在，无需迁移';
        RETURN;
    END IF;

    -- 检查表中是否有数据
    SELECT COUNT(*) > 0 INTO has_data FROM symbol_minute LIMIT 1;

    IF has_data THEN
        SELECT COUNT(*) INTO data_count FROM symbol_minute;
        RAISE NOTICE '⚠ 表中存在 % 条数据，将进行迁移', data_count;
    ELSE
        RAISE NOTICE '✓ 表中无数据，可以安全迁移';
    END IF;

    -- ========================================
    -- 步骤 2: 删除 TimescaleDB hypertable（临时）
    -- ========================================
    RAISE NOTICE '正在移除 TimescaleDB hypertable...';

    -- 将 hypertable 转换回普通表
    -- 注意：这会保留所有数据，但会移除分区
    PERFORM drop_chunks('symbol_minute', older_than => interval '-100 years');

    -- ========================================
    -- 步骤 3: 删除旧的主键约束
    -- ========================================
    RAISE NOTICE '正在删除旧的主键约束...';

    ALTER TABLE symbol_minute DROP CONSTRAINT IF EXISTS symbol_minute_pkey CASCADE;

    -- ========================================
    -- 步骤 4: 添加 frequency 字段
    -- ========================================
    RAISE NOTICE '正在添加 frequency 字段...';

    ALTER TABLE symbol_minute
    ADD COLUMN frequency VARCHAR(5);

    -- ========================================
    -- 步骤 5: 为现有数据设置默认值
    -- ========================================
    IF has_data THEN
        RAISE NOTICE '正在为现有数据设置默认 frequency=1m...';

        -- 假设现有数据都是 1 分钟数据
        UPDATE symbol_minute SET frequency = '1m' WHERE frequency IS NULL;

        RAISE NOTICE '✓ 已为 % 条记录设置 frequency=1m', data_count;
    END IF;

    -- ========================================
    -- 步骤 6: 设置字段为 NOT NULL
    -- ========================================
    RAISE NOTICE '正在设置 frequency 为 NOT NULL...';

    ALTER TABLE symbol_minute
    ALTER COLUMN frequency SET NOT NULL;

    -- ========================================
    -- 步骤 7: 创建新的复合主键
    -- ========================================
    RAISE NOTICE '正在创建新的复合主键...';

    ALTER TABLE symbol_minute
    ADD PRIMARY KEY (symbol, time, frequency);

    -- ========================================
    -- 步骤 8: 创建频率索引
    -- ========================================
    RAISE NOTICE '正在创建频率索引...';

    CREATE INDEX IF NOT EXISTS idx_symbol_minute_freq
    ON symbol_minute(frequency, symbol, time DESC);

    -- ========================================
    -- 步骤 9: 重新创建 TimescaleDB hypertable
    -- ========================================
    RAISE NOTICE '正在重新创建 TimescaleDB hypertable...';

    -- 重新创建 hypertable，使用复合分区
    PERFORM create_hypertable(
        'symbol_minute',
        'time',
        partitioning_column => 'frequency',
        number_partitions => 5,
        if_not_exists => TRUE,
        chunk_time_interval => INTERVAL '1 week',
        migrate_data => TRUE  -- 迁移现有数据
    );

    -- ========================================
    -- 步骤 10: 更新表和列注释
    -- ========================================
    RAISE NOTICE '正在更新表注释...';

    COMMENT ON TABLE symbol_minute IS '分钟数据表 - 存储股票分钟级K线数据（TimescaleDB超表，按时间和频率复合分区）';
    COMMENT ON COLUMN symbol_minute.frequency IS '数据频率：1m-1分钟, 5m-5分钟, 15m-15分钟, 30m-30分钟, 60m-60分钟';

    RAISE NOTICE '========================================';
    RAISE NOTICE '✓ 迁移完成！';
    RAISE NOTICE '========================================';
    RAISE NOTICE '主键已更新为: (symbol, time, frequency)';
    RAISE NOTICE '已添加索引: idx_symbol_minute_freq';
    RAISE NOTICE '已重新创建 TimescaleDB hypertable（时间+频率复合分区）';

END $$;
