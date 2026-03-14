-- ============================================================
-- 清理预处理周线/月线表中的重复数据
--
-- 问题描述：
--   由于时区处理问题，周线和月线表中存在同一日期的两条记录：
--   - 15:00:00+08:00 (旧数据，符合规范的收盘时间)
--   - 08:00:00+08:00 (新数据，错误格式)
--
-- 清理策略：
--   1. 删除所有 08:00:00 的重复记录
--   2. 对于只有 08:00 没有 15:00 的记录，更新时间为 15:00:00
--
-- 执行前请确认数据备份！
-- ============================================================

BEGIN;

-- ============================================================
-- 第一步：分析当前数据情况（仅供参考，不会修改数据）
-- ============================================================
DO $$
DECLARE
    weekly_08_count BIGINT;
    weekly_15_count BIGINT;
    weekly_only_08_count BIGINT;
    monthly_08_count BIGINT;
    monthly_15_count BIGINT;
    monthly_only_08_count BIGINT;
BEGIN
    -- 周线表统计
    SELECT COUNT(*) INTO weekly_08_count
    FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 8;

    SELECT COUNT(*) INTO weekly_15_count
    FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 15;

    SELECT COUNT(*) INTO weekly_only_08_count
    FROM (
        SELECT DISTINCT DATE(time) as date, symbol
        FROM processed_weekly_qfq
        WHERE EXTRACT(HOUR FROM time) = 8
        EXCEPT
        SELECT DISTINCT DATE(time) as date, symbol
        FROM processed_weekly_qfq
        WHERE EXTRACT(HOUR FROM time) = 15
    ) t;

    -- 月线表统计
    SELECT COUNT(*) INTO monthly_08_count
    FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 8;

    SELECT COUNT(*) INTO monthly_15_count
    FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 15;

    SELECT COUNT(*) INTO monthly_only_08_count
    FROM (
        SELECT DISTINCT DATE(time) as date, symbol
        FROM processed_monthly_qfq
        WHERE EXTRACT(HOUR FROM time) = 8
        EXCEPT
        SELECT DISTINCT DATE(time) as date, symbol
        FROM processed_monthly_qfq
        WHERE EXTRACT(HOUR FROM time) = 15
    ) t;

    RAISE NOTICE '========== 数据清理前统计 ==========';
    RAISE NOTICE '周线表: 08:00 记录数 = %, 15:00 记录数 = %, 仅08:00记录数 = %',
        weekly_08_count, weekly_15_count, weekly_only_08_count;
    RAISE NOTICE '月线表: 08:00 记录数 = %, 15:00 记录数 = %, 仅08:00记录数 = %',
        monthly_08_count, monthly_15_count, monthly_only_08_count;
    RAISE NOTICE '====================================';
END $$;

-- ============================================================
-- 第二步：处理周线表 (processed_weekly_qfq)
-- ============================================================

-- 2.1 先更新只有08:00记录的时间为15:00（这些是最新一周的数据，需要保留）
-- 使用临时表记录需要更新的记录
CREATE TEMP TABLE weekly_to_update AS
SELECT time, symbol
FROM processed_weekly_qfq
WHERE EXTRACT(HOUR FROM time) = 8
  AND DATE(time) IN (
      SELECT DATE(time) FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 8
      EXCEPT
      SELECT DATE(time) FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 15
  );

-- 更新这些记录的时间为 15:00:00
UPDATE processed_weekly_qfq
SET time = DATE(time) + TIME '15:00:00'
WHERE EXISTS (
    SELECT 1 FROM weekly_to_update w
    WHERE w.time = processed_weekly_qfq.time
      AND w.symbol = processed_weekly_qfq.symbol
);

DROP TABLE weekly_to_update;

-- 2.2 删除所有剩余的 08:00 记录（这些是重复数据，已有对应的15:00记录）
DELETE FROM processed_weekly_qfq
WHERE EXTRACT(HOUR FROM time) = 8;

-- ============================================================
-- 第三步：处理月线表 (processed_monthly_qfq)
-- ============================================================

-- 3.1 先更新只有08:00记录的时间为15:00
CREATE TEMP TABLE monthly_to_update AS
SELECT time, symbol
FROM processed_monthly_qfq
WHERE EXTRACT(HOUR FROM time) = 8
  AND DATE(time) IN (
      SELECT DATE(time) FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 8
      EXCEPT
      SELECT DATE(time) FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 15
  );

-- 更新这些记录的时间为 15:00:00
UPDATE processed_monthly_qfq
SET time = DATE(time) + TIME '15:00:00'
WHERE EXISTS (
    SELECT 1 FROM monthly_to_update m
    WHERE m.time = processed_monthly_qfq.time
      AND m.symbol = processed_monthly_qfq.symbol
);

DROP TABLE monthly_to_update;

-- 3.2 删除所有剩余的 08:00 记录
DELETE FROM processed_monthly_qfq
WHERE EXTRACT(HOUR FROM time) = 8;

-- ============================================================
-- 第四步：验证清理结果
-- ============================================================
DO $$
DECLARE
    weekly_08_count BIGINT;
    weekly_15_count BIGINT;
    monthly_08_count BIGINT;
    monthly_15_count BIGINT;
BEGIN
    SELECT COUNT(*) INTO weekly_08_count
    FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 8;

    SELECT COUNT(*) INTO weekly_15_count
    FROM processed_weekly_qfq WHERE EXTRACT(HOUR FROM time) = 15;

    SELECT COUNT(*) INTO monthly_08_count
    FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 8;

    SELECT COUNT(*) INTO monthly_15_count
    FROM processed_monthly_qfq WHERE EXTRACT(HOUR FROM time) = 15;

    RAISE NOTICE '========== 数据清理后统计 ==========';
    RAISE NOTICE '周线表: 08:00 记录数 = % (应为0), 15:00 记录数 = %',
        weekly_08_count, weekly_15_count;
    RAISE NOTICE '月线表: 08:00 记录数 = % (应为0), 15:00 记录数 = %',
        monthly_08_count, monthly_15_count;
    RAISE NOTICE '====================================';

    IF weekly_08_count > 0 OR monthly_08_count > 0 THEN
        RAISE WARNING '清理不完整，仍有 08:00 记录存在！';
    ELSE
        RAISE NOTICE '清理完成！所有记录已统一为 15:00:00 时间格式。';
    END IF;
END $$;

-- 提交事务
COMMIT;

-- ============================================================
-- 注意事项：
-- 1. 此脚本会修改数据，请确保有数据库备份
-- 2. 如果需要回滚，请在 COMMIT 前使用 ROLLBACK
-- 3. 清理后建议重新运行一次预处理命令以确保数据一致性：
--    fdh-cli preprocess run --all --category technical --freq weekly,monthly --force
-- ============================================================