from pathlib import Path
from decimal import Decimal
import json
from unittest.mock import AsyncMock, MagicMock, Mock, call

import numpy as np
import pandas as pd
import pytest

from finance_data_hub.preprocessing.mainline import (
    MainlinePreprocessor,
    calculate_leadlag_lasso,
    calculate_leadlag_monthly,
)
from finance_data_hub.database.operations import DataOperations
from finance_data_hub.cli.preprocess import _resolve_mainline_start_date
from finance_data_hub.providers.tushare import TushareProvider
from finance_data_hub.scheduler.executor import TaskExecutor
from finance_data_hub.scheduler.models import ScheduleConfig
from finance_data_hub.update.updater import DataUpdater


def test_stock_basic_none_fetches_all_listing_statuses():
    provider = TushareProvider(config={"token": "test-token"})

    def response(_api_name, **kwargs):
        status = kwargs["list_status"]
        return pd.DataFrame(
            {
                "ts_code": [
                    f"00000{'1' if status == 'L' else '2' if status == 'D' else '3'}.SZ"
                ],
                "name": [status],
                "market": ["主板"],
                "list_date": ["20100101"],
            }
        )

    provider._call_api = Mock(side_effect=response)
    result = provider.get_stock_basic(market="CN", list_status=None)

    assert set(result["list_status"]) == {"L", "D", "P"}
    assert [
        item.kwargs["list_status"] for item in provider._call_api.call_args_list
    ] == [
        "L",
        "D",
        "P",
    ]


def test_mainline_force_defaults_to_strategy_history_start():
    assert _resolve_mainline_start_date(None, force=True) == "2012-01-01"
    assert _resolve_mainline_start_date(None, force=False) is None
    assert _resolve_mainline_start_date("2020-01-01", force=True) == "2020-01-01"


def test_stock_st_uses_daily_snapshot_schema_and_documented_page_size():
    provider = TushareProvider(config={"token": "test-token"})
    provider._call_api = Mock(
        return_value=pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["ST示例"],
                "trade_date": ["20250813"],
                "type": ["ST"],
                "type_name": ["风险警示板"],
            }
        )
    )

    result = provider.get_stock_st(trade_date="2025-08-13")

    assert result.iloc[0]["trade_date"] == pd.Timestamp("2025-08-13")
    request = provider._call_api.call_args
    assert request.args[0] == "stock_st"
    assert request.kwargs["trade_date"] == "20250813"
    assert set(request.kwargs["fields"].split(",")) == {
        "ts_code",
        "name",
        "trade_date",
        "type",
        "type_name",
    }


def test_stock_dividend_supports_implementation_announcement_date_filter():
    provider = TushareProvider(config={"token": "test-token"})
    provider._get_mainline_series = Mock(return_value=pd.DataFrame())

    provider.get_stock_dividend(imp_ann_date="1991-03-05")

    params = provider._get_mainline_series.call_args.args[2]
    assert params["imp_ann_date"] == "1991-03-05"


def test_stock_st_splits_long_range_into_monthly_windows():
    provider = TushareProvider(config={"token": "test-token"})

    def response(_api_name, **kwargs):
        return pd.DataFrame(
            {
                "ts_code": ["000001.SZ"],
                "name": ["ST示例"],
                "trade_date": [kwargs["end_date"]],
                "type": ["ST"],
                "type_name": ["风险警示板"],
            }
        )

    provider._call_api = Mock(side_effect=response)
    result = provider.get_stock_st(start_date="2025-01-15", end_date="2025-03-02")

    assert len(result) == 3
    windows = [
        (item.kwargs["start_date"], item.kwargs["end_date"])
        for item in provider._call_api.call_args_list
    ]
    assert windows == [
        ("20250115", "20250131"),
        ("20250201", "20250228"),
        ("20250301", "20250302"),
    ]


@pytest.mark.parametrize(
    "method_name,kwargs,expected_frequency,expected_windows",
    [
        (
            "get_stock_suspend",
            {"start_date": "2025-01-30", "end_date": "2025-02-02"},
            "month",
            [("2025-01-30", "2025-01-31"), ("2025-02-01", "2025-02-02")],
        ),
        (
            "get_margin_detail",
            {"start_date": "2025-01-30", "end_date": "2025-02-01"},
            "day",
            [
                ("2025-01-30", "2025-01-30"),
                ("2025-01-31", "2025-01-31"),
                ("2025-02-01", "2025-02-01"),
            ],
        ),
        (
            "get_moneyflow_hsgt",
            {"start_date": "2024-12-30", "end_date": "2025-01-02"},
            "year",
            [("2024-12-30", "2024-12-31"), ("2025-01-01", "2025-01-02")],
        ),
    ],
)
def test_dense_mainline_datasets_use_bounded_date_windows(
    method_name, kwargs, expected_frequency, expected_windows
):
    provider = TushareProvider(config={"token": "test-token"})
    provider._get_mainline_date_chunks = Mock(return_value=pd.DataFrame())

    getattr(provider, method_name)(**kwargs)

    request = provider._get_mainline_date_chunks.call_args
    assert request.kwargs["chunk_frequency"] == expected_frequency
    # Verify the shared helper itself separately from each method's routing.
    provider._get_mainline_date_chunks = (
        TushareProvider._get_mainline_date_chunks.__get__(provider, TushareProvider)
    )
    provider._get_mainline_series = Mock(return_value=pd.DataFrame())
    getattr(provider, method_name)(**kwargs)
    windows = [
        (item.args[2]["start_date"], item.args[2]["end_date"])
        for item in provider._get_mainline_series.call_args_list
    ]
    assert windows == expected_windows


def test_moneyflow_stock_history_uses_one_request_per_stock_range():
    provider = TushareProvider(config={"token": "test-token"})
    provider._get_mainline_date_chunks = Mock(return_value=pd.DataFrame())
    provider._get_mainline_series = Mock(return_value=pd.DataFrame())

    provider.get_moneyflow(
        ts_code="000001.SZ", start_date="2010-01-01", end_date="2025-02-01"
    )

    provider._get_mainline_date_chunks.assert_not_called()
    request = provider._get_mainline_series.call_args
    assert request.args[0] == "moneyflow"
    assert request.args[2] == {
        "ts_code": "000001.SZ",
        "trade_date": None,
        "start_date": "2010-01-01",
        "end_date": "2025-02-01",
    }


@pytest.mark.asyncio
async def test_mainline_updater_routes_and_persists_raw_dataset():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(
            return_value=pd.DataFrame(
                {
                    "trade_date": [pd.Timestamp("2025-08-13")],
                    "north_money": [12.3],
                }
            )
        )
    )
    updater.data_ops = Mock(upsert_mainline_raw_batch=AsyncMock(return_value=1))

    count = await updater.update_mainline_raw("moneyflow_hsgt", trade_date="2025-08-13")

    assert count == 1
    updater.router.route.assert_called_once_with(
        asset_class="stock",
        data_type="moneyflow_hsgt",
        method_name="get_moneyflow_hsgt",
        market="CN",
        trade_date="2025-08-13",
    )
    updater.data_ops.upsert_mainline_raw_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_margin_detail_range_reports_date_progress_and_commits_each_day():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(
            side_effect=[
                pd.DataFrame(),
                pd.DataFrame(
                    {
                        "ts_code": ["000001.SZ"],
                        "trade_date": [pd.Timestamp("2025-01-31")],
                    }
                ),
                pd.DataFrame(),
            ]
        )
    )
    updater.data_ops = Mock(upsert_mainline_raw_batch=AsyncMock(return_value=1))
    progress = Mock()

    count = await updater.update_mainline_raw(
        "margin_detail",
        start_date="2025-01-30",
        end_date="2025-02-01",
        progress_callback=progress,
    )

    assert count == 1
    assert [
        item.kwargs["trade_date"] for item in updater.router.route.call_args_list
    ] == ["2025-01-30", "2025-01-31", "2025-02-01"]
    assert all(item.kwargs["data_type"] == "margin_detail" for item in updater.router.route.call_args_list)
    assert progress.call_args_list == [
        call(0, 3),
        call(1, 3),
        call(2, 3),
        call(3, 3),
    ]
    updater.data_ops.upsert_mainline_raw_batch.assert_awaited_once()


@pytest.mark.asyncio
async def test_moneyflow_full_backfill_uses_stock_universe_and_exchange_floors():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(
            return_value=pd.DataFrame(
                {
                    "ts_code": ["000001.SZ"],
                    "trade_date": [pd.Timestamp("2010-01-04")],
                    "net_mf_amount": [1.0],
                }
            )
        )
    )
    updater.data_ops = Mock(
        get_asset_basic=AsyncMock(
            return_value=pd.DataFrame(
                {
                    "symbol": ["600001.SH", "000001.SZ"],
                    "list_date": ["2008-01-01", "1991-04-03"],
                }
            )
        ),
        upsert_mainline_raw_batch=AsyncMock(return_value=1),
    )
    progress = Mock()

    count = await updater.update_mainline_raw(
        "moneyflow", end_date="2011-01-31", progress_callback=progress
    )

    assert count == 2
    assert [item.kwargs["ts_code"] for item in updater.router.route.call_args_list] == [
        "000001.SZ",
        "600001.SH",
    ]
    assert [item.kwargs["start_date"] for item in updater.router.route.call_args_list] == [
        "2010-01-01",
        "2008-01-01",
    ]
    assert all(
        item.kwargs["end_date"] == "2011-01-31"
        for item in updater.router.route.call_args_list
    )
    assert progress.call_args_list == [call(0, 2), call(1, 2), call(2, 2)]


@pytest.mark.asyncio
async def test_dividend_full_backfill_queries_complete_ldp_stock_pool():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(route=Mock(return_value=pd.DataFrame()))
    updater.data_ops = Mock(
        get_asset_basic=AsyncMock(
            return_value=pd.DataFrame(
                {
                    "symbol": ["000001.SZ", "600001.SH", "600001.SH"],
                    "list_status": ["L", "D", "D"],
                }
            )
        )
    )
    progress = Mock()

    count = await updater.update_mainline_raw(
        "stock_dividend", progress_callback=progress
    )

    assert count == 0
    assert [
        item.kwargs["ts_code"] for item in updater.router.route.call_args_list
    ] == ["000001.SZ", "600001.SH"]
    assert all(
        item.kwargs["method_name"] == "get_stock_dividend"
        for item in updater.router.route.call_args_list
    )
    assert progress.call_args_list == [call(0, 2), call(1, 2), call(2, 2)]


@pytest.mark.asyncio
async def test_dividend_missing_ann_date_uses_point_in_time_safe_fallback():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(
            return_value=pd.DataFrame(
                {
                    "ts_code": ["000003.SZ", "000003.SZ"],
                    "end_date": pd.to_datetime(["1990-12-31", "1989-12-31"]),
                    "ann_date": [pd.NaT, pd.NaT],
                    "imp_ann_date": [pd.Timestamp("1991-03-05"), pd.NaT],
                    "div_proc": ["实施", "实施"],
                }
            )
        )
    )
    updater.data_ops = Mock(upsert_mainline_raw_batch=AsyncMock(return_value=1))

    count = await updater.update_mainline_raw(
        "stock_dividend", symbols=["000003.SZ"]
    )

    assert count == 1
    stored = updater.data_ops.upsert_mainline_raw_batch.await_args.args[1]
    assert len(stored) == 1
    assert stored.iloc[0]["ann_date"] == pd.Timestamp("1991-03-05")


@pytest.mark.asyncio
async def test_dividend_daily_update_merges_preliminary_and_implementation_dates():
    updater = object.__new__(DataUpdater)
    updater.router = Mock(
        route=Mock(
            side_effect=[
                pd.DataFrame(
                    {
                        "ts_code": ["000001.SZ"],
                        "end_date": pd.to_datetime(["2024-12-31"]),
                        "ann_date": pd.to_datetime(["2025-03-01"]),
                        "imp_ann_date": [pd.NaT],
                        "div_proc": ["预披露"],
                    }
                ),
                pd.DataFrame(
                    {
                        "ts_code": ["000003.SZ"],
                        "end_date": pd.to_datetime(["1990-12-31"]),
                        "ann_date": [pd.NaT],
                        "imp_ann_date": pd.to_datetime(["2025-03-01"]),
                        "div_proc": ["实施"],
                    }
                ),
            ]
        )
    )
    updater.data_ops = Mock(upsert_mainline_raw_batch=AsyncMock(return_value=2))

    count = await updater.update_mainline_raw(
        "stock_dividend", trade_date="2025-03-01"
    )

    assert count == 2
    assert updater.router.route.call_args_list == [
        call(
            asset_class="stock",
            data_type="stock_dividend",
            method_name="get_stock_dividend",
            market="CN",
            ann_date="2025-03-01",
        ),
        call(
            asset_class="stock",
            data_type="stock_dividend",
            method_name="get_stock_dividend",
            market="CN",
            imp_ann_date="2025-03-01",
        ),
    ]
    stored = updater.data_ops.upsert_mainline_raw_batch.await_args.args[1]
    assert len(stored) == 2
    assert stored["ann_date"].notna().all()


@pytest.mark.asyncio
async def test_mainline_upsert_counts_successful_asyncpg_executemany_records():
    manager = Mock()
    manager._engine = Mock()
    connection = AsyncMock()
    transaction = MagicMock()
    transaction.__aenter__ = AsyncMock(return_value=connection)
    transaction.__aexit__ = AsyncMock(return_value=False)
    manager._engine.begin.return_value = transaction
    connection.execute.return_value = Mock(rowcount=-1)
    operations = DataOperations(manager)

    count = await operations.upsert_mainline_raw_batch(
        "moneyflow_hsgt",
        pd.DataFrame(
            {
                "trade_date": pd.to_datetime(["2025-08-14", "2025-08-15"]),
                "north_money": [1.0, 2.0],
            }
        ),
    )

    assert count == 2


@pytest.mark.asyncio
async def test_mainline_preprocessor_commits_monthly_partitions_with_progress():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._materialize_stock = AsyncMock(return_value=2)
    preprocessor._materialize_market = AsyncMock(return_value=1)
    preprocessor._materialize_industry = AsyncMock(return_value=3)
    preprocessor._materialize_etf = AsyncMock(return_value=4)
    preprocessor._materialize_crowding = AsyncMock(return_value=5)
    preprocessor._refresh_status = AsyncMock(return_value=4)
    progress = Mock()

    counts = await preprocessor.run(
        start_date="2025-01-15",
        end_date="2025-03-02",
        include_monthly=True,
        progress_callback=progress,
    )

    assert counts == {
        "stock_daily": 6,
        "market_daily": 3,
        "industry_daily": 9,
        "etf_daily": 12,
        "fund_crowding_monthly": 15,
    }
    assert preprocessor._materialize_stock.await_args_list == [
        call(pd.Timestamp("2025-01-15").date(), pd.Timestamp("2025-01-31").date()),
        call(pd.Timestamp("2025-02-01").date(), pd.Timestamp("2025-02-28").date()),
        call(pd.Timestamp("2025-03-01").date(), pd.Timestamp("2025-03-02").date()),
    ]
    assert progress.call_args_list[0].args[:2] == (0, 16)
    assert progress.call_args_list[-1].args == (16, 16, "data_status")


def test_mainline_long_backfill_uses_six_month_partitions():
    preprocessor = MainlinePreprocessor(Mock())

    partitions = preprocessor._build_partitions(
        pd.Timestamp("2012-01-01").date(),
        pd.Timestamp("2014-01-15").date(),
    )

    assert partitions == [
        (pd.Timestamp("2012-01-01").date(), pd.Timestamp("2012-06-30").date()),
        (pd.Timestamp("2012-07-01").date(), pd.Timestamp("2012-12-31").date()),
        (pd.Timestamp("2013-01-01").date(), pd.Timestamp("2013-06-30").date()),
        (pd.Timestamp("2013-07-01").date(), pd.Timestamp("2013-12-31").date()),
        (pd.Timestamp("2014-01-01").date(), pd.Timestamp("2014-01-15").date()),
    ]


@pytest.mark.asyncio
async def test_mainline_etf_recovery_stage_skips_other_daily_materializers():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._materialize_etf = AsyncMock(return_value=4)
    preprocessor._materialize_etf_exposure = AsyncMock(return_value=6)
    preprocessor._materialize_stock = AsyncMock()
    preprocessor._materialize_market = AsyncMock()
    preprocessor._materialize_industry = AsyncMock()

    counts = await preprocessor.run(
        start_date="2026-07-01", end_date="2026-08-21", stages=["etf"]
    )

    assert counts == {"etf_daily": 8, "etf_exposure_monthly": 12}
    preprocessor._materialize_stock.assert_not_awaited()
    preprocessor._materialize_market.assert_not_awaited()
    preprocessor._materialize_industry.assert_not_awaited()


@pytest.mark.asyncio
async def test_mainline_industry_recovery_stage_skips_other_materializers():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._materialize_industry = AsyncMock(return_value=3)
    preprocessor._materialize_stock = AsyncMock()
    preprocessor._materialize_market = AsyncMock()
    preprocessor._materialize_etf = AsyncMock()

    counts = await preprocessor.run(
        start_date="2026-08-20", end_date="2026-08-20", stages=["industry"]
    )

    assert counts == {"industry_daily": 3}
    preprocessor._materialize_stock.assert_not_awaited()
    preprocessor._materialize_market.assert_not_awaited()
    preprocessor._materialize_etf.assert_not_awaited()


@pytest.mark.asyncio
async def test_mainline_stock_recovery_stage_skips_other_materializers():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._materialize_stock = AsyncMock(return_value=7)
    preprocessor._materialize_market = AsyncMock()
    preprocessor._materialize_industry = AsyncMock()
    preprocessor._materialize_etf = AsyncMock()

    counts = await preprocessor.run(
        start_date="2026-08-20", end_date="2026-08-20", stages=["stock"]
    )

    assert counts == {"stock_daily": 7}
    preprocessor._materialize_market.assert_not_awaited()
    preprocessor._materialize_industry.assert_not_awaited()
    preprocessor._materialize_etf.assert_not_awaited()


@pytest.mark.asyncio
async def test_mainline_market_recovery_stage_skips_other_materializers():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._materialize_market = AsyncMock(return_value=1)
    preprocessor._materialize_stock = AsyncMock()
    preprocessor._materialize_industry = AsyncMock()
    preprocessor._materialize_etf = AsyncMock()

    counts = await preprocessor.run(
        start_date="2026-08-20", end_date="2026-08-20", stages=["market"]
    )

    assert counts == {"market_daily": 1}
    preprocessor._materialize_stock.assert_not_awaited()
    preprocessor._materialize_industry.assert_not_awaited()
    preprocessor._materialize_etf.assert_not_awaited()


@pytest.mark.asyncio
async def test_mainline_stock_sql_preserves_time_index_predicates():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._execute = AsyncMock(return_value=1)

    await preprocessor._materialize_stock(
        pd.Timestamp("2025-01-01").date(), pd.Timestamp("2025-01-31").date()
    )

    sql = preprocessor._execute.await_args_list[0].args[0]
    assert "d.time::date BETWEEN" not in sql
    assert "db.time::date BETWEEN" not in sql
    assert "LEFT JOIN v_fundamental_combined" not in sql
    assert "FROM processed_valuation_pct pv" in sql
    assert "ic.index_code=m.l2_code" in sql


@pytest.mark.asyncio
async def test_mainline_etf_sql_marks_missing_list_date_not_tradable():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._execute = AsyncMock(return_value=1)

    await preprocessor._materialize_etf(
        pd.Timestamp("2026-07-01").date(), pd.Timestamp("2026-08-21").date()
    )

    sql = preprocessor._execute.await_args.args[0]
    assert "COALESCE(list_date<=trade_date,FALSE)" in sql
    assert "missing_list_date" in sql


@pytest.mark.asyncio
async def test_mainline_status_casts_json_parameter_for_asyncpg():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._execute = AsyncMock(return_value=4)

    await preprocessor._refresh_status(pd.Timestamp("2025-01-31").date())

    sql = preprocessor._execute.await_args.args[0]
    assert "CAST(:benchmark AS text)" in sql


@pytest.mark.asyncio
async def test_mainline_crowding_limits_incremental_runs_to_recently_written_periods():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._execute = AsyncMock(return_value=2)

    await preprocessor._materialize_crowding(
        pd.Timestamp("2026-08-11").date(),
        pd.Timestamp("2026-08-21").date(),
        source_updated_since="2026-08-11",
    )

    sql, params = preprocessor._execute.await_args.args
    assert "WITH impacted_periods AS" in sql
    assert "updated_at >= CAST(:source_updated_since AS date)" in sql
    assert "end_date IN (SELECT end_date FROM impacted_periods)" in sql
    assert params["source_updated_since"] == "2026-08-11"


@pytest.mark.asyncio
async def test_mainline_industry_crowding_uses_the_same_incremental_source_window():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._execute = AsyncMock(return_value=2)

    await preprocessor._materialize_industry_crowding(
        pd.Timestamp("2026-08-11").date(),
        pd.Timestamp("2026-08-21").date(),
        source_updated_since="2026-08-11",
    )

    sql, params = preprocessor._execute.await_args.args
    assert "WITH impacted_periods AS" in sql
    assert "fp.end_date IN (SELECT end_date FROM impacted_periods)" in sql
    assert params["source_updated_since"] == "2026-08-11"


@pytest.mark.asyncio
async def test_mainline_publish_snapshot_reads_dataset_coverage_without_tuple_attributes():
    preprocessor = MainlinePreprocessor(Mock())
    preprocessor._read_dataframe = AsyncMock(return_value=pd.DataFrame([
        {
            "dataset": "stock_daily", "partition_date": pd.Timestamp("2026-08-21").date(),
            "completeness": 1.0, "status": "ready", "blocker_reasons": [], "details": {},
        },
        {
            "dataset": "industry_daily", "partition_date": pd.Timestamp("2026-08-21").date(),
            "completeness": 0.98, "status": "ready", "blocker_reasons": [], "details": {},
        },
    ]))
    preprocessor._execute = AsyncMock(return_value=1)

    count = await preprocessor._publish_snapshot(pd.Timestamp("2026-08-21").date())

    assert count == 1
    params = preprocessor._execute.await_args.args[1]
    assert json.loads(params["coverage"]) == {"stock_daily": 1.0, "industry_daily": 0.98}
    assert json.loads(params["watermark"]) == {"market": "2026-08-21"}
    assert "dataset,partition_date,completeness" in preprocessor._read_dataframe.await_args.args[0]
    assert "CAST(:status AS VARCHAR(16))" in preprocessor._execute.await_args.args[0]
    assert "CAST(:coverage AS JSONB)" in preprocessor._execute.await_args.args[0]
    assert "ON CONFLICT(factor_version,as_of_trade_date) DO UPDATE SET" in preprocessor._execute.await_args.args[0]


def test_leadlag_calculator_finds_known_delay():
    dates = pd.date_range("2025-01-01", periods=80, freq="D")
    rng = np.random.default_rng(42)
    leader = rng.normal(size=len(dates))
    follower = np.r_[np.full(3, np.nan), leader[:-3]]
    left = pd.DataFrame({"trade_date": dates, "code": "L1", "return": leader})
    right = pd.DataFrame({"trade_date": dates, "code": "L2", "return": follower})

    result = calculate_leadlag_monthly(left, right, max_lag_days=8)

    assert result.iloc[0]["best_lag_days"] == 3
    assert result.iloc[0]["correlation"] == pytest.approx(1.0)


def test_leadlag_lasso_accepts_decimal_database_values():
    dates = pd.date_range("2020-01-01", periods=300, freq="B")
    rows = []
    for index, trade_date in enumerate(dates):
        rows.extend([
            {
                "trade_date": trade_date,
                "l2_code": "801010.SI",
                "relative_return_20d": Decimal(str(index / 10_000)),
            },
            {
                "trade_date": trade_date,
                "l2_code": "801020.SI",
                "relative_return_20d": Decimal(str((index % 17) / 10_000)),
            },
        ])

    relation, score = calculate_leadlag_lasso(pd.DataFrame(rows), dates[-1])

    assert not relation.empty
    assert set(score["l2_code"]) == {"801010.SI", "801020.SI"}


def test_scheduler_accepts_mainline_jobs_and_dependencies():
    root = Path(__file__).resolve().parents[2]
    config = ScheduleConfig.from_yaml(str(root / "schedules.yml"))

    basic = config.jobs["basic_update"]
    assert basic.params == {
        "asset_class": "stock",
        "market": "CN",
        "force": True,
    }

    for job_id in ("sw_classify_update", "sw_member_update"):
        assert config.jobs[job_id].params == {
            "asset_class": "index",
            "force": True,
        }

    assert not {
        "stock_st_history_update",
        "stock_suspend_history_update",
        "stock_dividend_history_update",
        "stock_repurchase_history_update",
        "margin_detail_history_update",
        "moneyflow_hsgt_history_update",
        "moneyflow_history_update",
        "mainline_history_preprocess",
    }.intersection(config.jobs)

    daily_mainline = config.jobs["mainline_preprocess"]
    assert daily_mainline.category == "mainline"
    assert {
        "technical_preprocess",
        "fundamental_preprocess",
        "industry_valuation_preprocess",
        "stock_st_update",
        "stock_suspend_update",
        "stock_dividend_update",
        "stock_repurchase_update",
        "margin_detail_update",
        "moneyflow_update",
    }.issubset(daily_mainline.depends_on)
    assert "moneyflow_hsgt_update" in daily_mainline.depends_on
    assert "etf_share_size_catchup" in daily_mainline.depends_on
    assert "etf_share_size_update" not in daily_mainline.depends_on
    assert "index_weight_update" not in daily_mainline.depends_on
    assert daily_mainline.params["start_date"] == "latest-10bd"

    crowding_mainline = config.jobs["mainline_crowding_preprocess"]
    assert crowding_mainline.schedule["hour"] == 23
    assert crowding_mainline.schedule["minute"] == 25
    assert crowding_mainline.params["stage"] == "crowding"
    assert crowding_mainline.params["start_date"] == "latest-10bd"
    assert crowding_mainline.params["source_updated_since"] == "latest-10bd"

    leadlag_mainline = config.jobs["mainline_leadlag_month_end"]
    assert leadlag_mainline.schedule == {
        "type": "cron", "day": "1-7", "day_of_week": "mon", "hour": 23, "minute": 55
    }
    assert leadlag_mainline.params["end_date"] == "previous_month_last_trade_date"
    assert config.jobs["etf_share_size_catchup"].params["trade_date"] == "latest"


def test_scheduler_forwards_mainline_stage_to_preprocess_cli(tmp_path):
    executor = TaskExecutor(project_root=str(tmp_path), python_path="python")

    command = executor._build_preprocess_command(
        "mainline",
        {
            "all": True,
            "stage": "crowding",
            "start_date": "2026-08-11",
            "source_updated_since": "2026-08-11",
        },
    )

    assert command == [
        "python", "-m", "finance_data_hub.cli.main", "preprocess", "run",
        "--category", "mainline", "--all", "--stage", "crowding",
        "--start-date", "2026-08-11", "--source-updated-since", "2026-08-11",
    ]


def test_scheduler_resolves_previous_month_last_trade_date(monkeypatch, tmp_path):
    executor = TaskExecutor(project_root=str(tmp_path), python_path="python")
    monkeypatch.setattr(
        executor,
        "_query_trade_calendar_date",
        Mock(return_value="2026-07-31"),
    )

    assert executor._resolve_date_param("previous_month_last_trade_date") == "2026-07-31"


def test_mainline_migration_keeps_strategy_scoring_outside_data_hub():
    root = Path(__file__).resolve().parents[2]
    sql = (root / "sql/migrations/037_create_mainline_strategy_data.sql").read_text()

    assert "uq_sw_member_history" in sql
    assert "processed_mainline_stock_daily" in sql
    assert "processed_mainline_etf_daily" in sql
    assert "strategy_score" not in sql.lower()


def test_standard_moneyflow_migration_only_adds_the_approved_source():
    root = Path(__file__).resolve().parents[2]
    sql = (root / "sql/migrations/040_add_mainline_standard_moneyflow.sql").read_text()

    assert "CREATE TABLE IF NOT EXISTS moneyflow" in sql
    assert "moneyflow_net_amount_20d" in sql
    assert "moneyflow_ths" not in sql
    assert "moneyflow_dc" not in sql
