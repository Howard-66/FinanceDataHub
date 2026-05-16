"""
期货期限结构预处理集成测试

使用真实的 Tushare + PostgreSQL/TimescaleDB 链路，验证：
1. 真实期货日线/结算数据可写入 futures schema
2. preprocess_futures_term_data 能生成 term_structure / term_spread / roll_yield
3. 三张衍生表之间的关键字段保持一致
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
                for table in ("term_structure", "term_spread", "roll_yield"):
                    await conn.execute(
                        text(
                            f"""
                            DELETE FROM futures.{table}
                            WHERE product_code = 'RB'
                              AND time::date = DATE '2024-04-30'
                            """
                        )
                    )

            assert await ops.insert_futures_daily_batch(rb_daily) >= 2
            assert await ops.insert_futures_settle_batch(rb_settle) >= 2

            counts = await updater.preprocess_futures_term_data(
                product_codes=["RB"],
                start_date="2024-04-30",
                end_date="2024-04-30",
            )
            assert counts["term_structure"] >= 1
            assert counts["term_spread"] >= 1
            assert counts["roll_yield"] >= 1

            term = await ops.get_futures_term_structure(["RB"], "2024-04-30", "2024-04-30")
            spread = await ops.get_futures_term_spread(["RB"], "2024-04-30", "2024-04-30")
            roll = await ops.get_futures_roll_yield(["RB"], "2024-04-30", "2024-04-30")

            assert term is not None and len(term) == 1
            assert spread is not None and len(spread) == 1
            assert roll is not None and len(roll) == 1

            term_row = term.iloc[0]
            spread_row = spread.iloc[0]
            roll_row = roll.iloc[0]

            assert term_row["product_code"] == "RB"
            assert term_row["exchange"] == "SHFE"
            assert term_row["primary_contract"]
            assert term_row["secondary_contract"]
            assert int(term_row["candidate_count"]) >= 2

            assert spread_row["product_code"] == "RB"
            assert spread_row["primary_contract"] == term_row["primary_contract"]
            assert spread_row["secondary_contract"] == term_row["secondary_contract"]
            assert spread_row["spread"] == pytest.approx(
                spread_row["primary_contract_close"] - spread_row["secondary_contract_close"]
            )

            assert roll_row["product_code"] == "RB"
            assert roll_row["primary_contract"] == spread_row["primary_contract"]
            assert roll_row["secondary_contract"] == spread_row["secondary_contract"]
            assert roll_row["spread"] == pytest.approx(spread_row["spread"])
            assert int(roll_row["days_between_expiry"]) > 0
            assert roll_row["annualized_roll_yield"] is not None
        finally:
            await updater.close()
            await db_manager.close()

    asyncio.run(_run())
