"""
期货期限结构预处理集成测试

使用真实的 Tushare + PostgreSQL/TimescaleDB 链路，验证：
1. 真实期货日线/结算数据可写入 futures schema
2. preprocess_futures_term_metrics 能生成 term_metrics
3. 期限结构、跨期价差和展期收益率字段保持一致
"""

import asyncio

import pytest
from sqlalchemy import text

from finance_data_hub.config import get_settings
from finance_data_hub.database.manager import DatabaseManager
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.providers.tushare import TushareProvider
from finance_data_hub.update.updater import DataUpdater


@pytest.mark.integration
def test_futures_term_preprocess_real_chain():
    async def _run() -> None:
        settings = get_settings()
        token = settings.data_source.tushare_token
        if not token:
            pytest.skip("TUSHARE_TOKEN 未配置，跳过真实期货预处理集成测试")

        provider = TushareProvider(config={"token": token})
        provider.initialize()

        daily = provider.get_futures_daily(trade_date="2024-04-30", exchange="SHFE")
        rb_daily = daily[daily["product_code"] == "RB"].copy()
        assert len(rb_daily) >= 2, "RB 应至少返回两条可形成曲线的真实日线"

        settle = provider.get_futures_settle(trade_date="2024-04-30")
        rb_settle = settle[settle["product_code"] == "RB"].copy()
        assert len(rb_settle) >= 2, "RB 应至少返回两条真实结算参数"

        db_manager = DatabaseManager(settings)
        await db_manager.initialize()
        ops = DataOperations(db_manager)

        updater = DataUpdater(settings)
        await updater.initialize()
        try:
            async with db_manager._engine.begin() as conn:
                await conn.execute(
                    text(
                        """
                        DELETE FROM futures.term_metrics
                        WHERE product_code = 'RB'
                          AND time::date = DATE '2024-04-30'
                        """
                    )
                )

            assert await ops.insert_futures_daily_batch(rb_daily) >= 2
            assert await ops.insert_futures_settle_batch(rb_settle) >= 2

            count = await updater.preprocess_futures_term_metrics(
                product_codes=["RB"],
                start_date="2024-04-30",
                end_date="2024-04-30",
            )
            assert count >= 1

            metrics = await ops.get_futures_term_metrics(["RB"], "2024-04-30", "2024-04-30")

            assert metrics is not None and len(metrics) == 1

            row = metrics.iloc[0]

            assert row["product_code"] == "RB"
            assert row["exchange"] == "SHFE"
            assert row["primary_contract"]
            assert row["secondary_contract"]
            assert int(row["candidate_count"]) >= 2
            assert row["spread"] == pytest.approx(
                row["primary_contract_close"] - row["secondary_contract_close"]
            )
            assert int(row["days_between_expiry"]) > 0
            assert row["annualized_roll_yield"] is not None
        finally:
            await updater.close()
            await db_manager.close()

    asyncio.run(_run())
