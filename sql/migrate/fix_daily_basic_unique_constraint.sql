-- 修复 daily_basic 表：添加唯一约束和修复列名
-- 此迁移脚本解决 ON CONFLICT 语句需要的唯一约束问题

BEGIN;

-- 检查表是否存在
DO $$
BEGIN
    -- 如果存在 turnover_rate_f 列，则重命名为 volume_ratio
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'daily_basic'
        AND column_name = 'turnover_rate_f'
    ) THEN
        -- 重命名列
        ALTER TABLE daily_basic RENAME COLUMN turnover_rate_f TO volume_ratio;

        -- 注释列
        COMMENT ON COLUMN daily_basic.volume_ratio IS '量比';
    END IF;

    -- 检查唯一约束是否存在
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'daily_basic'
        AND constraint_type = 'UNIQUE'
        AND constraint_name = 'daily_basic_symbol_time_key'
    ) THEN
        -- 添加唯一约束
        ALTER TABLE daily_basic ADD CONSTRAINT daily_basic_symbol_time_key UNIQUE (symbol, time);
        RAISE NOTICE '已添加唯一约束 daily_basic_symbol_time_key';
    ELSE
        RAISE NOTICE '唯一约束 daily_basic_symbol_time_key 已存在';
    END IF;
END
$$;

COMMIT;
