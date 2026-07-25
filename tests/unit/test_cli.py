"""
CLI 模块单元测试
"""

import asyncio
import concurrent.futures
import pytest
from unittest.mock import ANY, AsyncMock, Mock, patch
from typer.testing import CliRunner
import pandas as pd

from finance_data_hub.cli.main import app


runner = CliRunner()


def test_cli_help():
    """测试 CLI 帮助命令"""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "fdh-cli" in result.output
    assert "update" in result.output
    assert "etl" in result.output
    assert "status" in result.output


def test_cli_version():
    """测试 CLI 版本命令"""
    result = runner.invoke(app, ["--version"])
    assert result.exit_code == 0
    assert "FinanceDataHub CLI" in result.output


def test_cli_status():
    """测试状态命令"""
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "PostgreSQL" in result.output
    assert "Redis" in result.output


def test_cli_status_verbose():
    """测试详细状态命令"""
    result = runner.invoke(app, ["status", "--verbose"])
    assert result.exit_code == 0
    assert "PostgreSQL" in result.output
    assert "详细信息" in result.output


def test_cli_update():
    """测试更新命令 - 使用mock避免真实数据更新"""
    # 使用mock避免真实调用provider
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, ["update"])
        # CLI应该成功启动，即使update失败
        assert result.exit_code == 0
        assert "开始数据更新流程" in result.output


def test_cli_update_with_options():
    """测试带选项的更新命令"""
    result = runner.invoke(app, [
        "update",
        "--asset-class", "stock",
        "--frequency", "daily",
        "--symbols", "600519.SH,000858.SZ"
    ])
    assert result.exit_code == 0
    assert "资产类别: stock" in result.output
    assert "数据频率: daily" in result.output


def test_cli_etl():
    """测试 ETL 命令"""
    result = runner.invoke(app, ["etl"])
    assert result.exit_code == 0
    assert "开始 ETL 流程" in result.output


def test_cli_etl_with_options():
    """测试带选项的 ETL 命令"""
    result = runner.invoke(app, [
        "etl",
        "--from-date", "2024-01-01",
        "--to-date", "2024-12-31",
        "--dry-run"
    ])
    assert result.exit_code == 0
    assert "开始日期: 2024-01-01" in result.output
    assert "结束日期: 2024-12-31" in result.output
    assert "试运行模式" in result.output


def test_cli_config():
    """测试配置命令"""
    result = runner.invoke(app, ["config"])
    assert result.exit_code == 0
    assert "当前配置" in result.output
    assert "数据库配置" in result.output


def test_cli_config_reload():
    """测试重新加载配置"""
    result = runner.invoke(app, ["config", "--reload"])
    assert result.exit_code == 0
    assert "配置已重新加载" in result.output


def test_cli_update_with_dataset():
    """测试使用 --dataset 参数的更新命令"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH"
        ])
        assert result.exit_code == 0
        assert "数据类型: daily" in result.output
        assert "智能下载" in result.output


def test_cli_update_with_force():
    """测试强制更新模式"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--force"
        ])
        assert result.exit_code == 0
        assert "强制更新" in result.output


def test_cli_update_with_force_and_date_range():
    """测试强制更新模式配合日期范围"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--start-date", "2020-01-01",
            "--end-date", "2024-12-31"
        ])
        assert result.exit_code == 0
        assert "强制更新" in result.output
        assert "开始日期: 2020-01-01" in result.output
        assert "结束日期: 2024-12-31" in result.output


def test_cli_update_with_trade_date():
    """测试交易日批量更新模式"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--trade-date", "2024-11-18"
        ])
        assert result.exit_code == 0
        assert "交易日: 2024-11-18" in result.output
        assert "交易日批量更新模式" in result.output


def test_cli_update_smart_download_no_symbols():
    """测试智能下载模式 - 无symbol参数（全资产更新）"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        with patch('finance_data_hub.update.updater.DataUpdater.data_ops') as mock_ops:
            mock_ops.get_symbol_list.return_value = ["600519.SH", "000858.SZ"]
            result = runner.invoke(app, [
                "update",
                "--dataset", "daily"
            ])
            assert result.exit_code == 0
            assert "智能下载" in result.output


def test_cli_update_with_verbose():
    """测试详细输出模式"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--verbose"
        ])
        assert result.exit_code == 0
        assert "verbose" in result.output.lower() or "详细" in result.output


def test_cli_update_strategy_matrix_force_takes_precedence():
    """测试策略矩阵 - force参数优先级"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        # 当同时提供 force 和 start_date 时，应该是强制更新模式
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--force",
            "--start-date", "2020-01-01"
        ])
        assert result.exit_code == 0
        assert "强制更新" in result.output


def test_cli_update_strategy_matrix_trade_date_priority():
    """测试策略矩阵 - trade_date优先级最高"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        # 当同时提供 trade_date 和其他参数时，trade_date优先级最高
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--trade-date", "2024-11-18"
        ])
        assert result.exit_code == 0
        assert "交易日批量更新模式" in result.output


def test_cli_update_deprecated_frequency_warning():
    """测试 --frequency 参数废弃警告"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--frequency", "daily"
        ])
        assert result.exit_code == 0
        assert "警告" in result.output
        assert "已废弃" in result.output


def test_cli_update_missing_dataset_and_frequency():
    """测试缺少数据类型参数时的错误处理"""
    result = runner.invoke(app, ["update"])
    assert result.exit_code != 0
    assert "必须指定数据类型" in result.output


def test_cli_update_adj_parameter():
    """测试复权类型参数"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily",
            "--symbols", "600519.SH",
            "--adj", "qfq"
        ])
        assert result.exit_code == 0
        assert "复权类型: qfq" in result.output


def test_cli_update_minute_data():
    """测试分钟数据更新"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "minute_1",
            "--symbols", "600519.SH"
        ])
        assert result.exit_code == 0
        assert "数据类型: minute_1" in result.output


def test_cli_update_future_symbols_all_cannot_mix_with_other_codes():
    result = runner.invoke(
        app,
        [
            "update",
            "--asset-class", "future",
            "--dataset", "daily",
            "--symbols", "all,RB2405.SHF",
        ],
    )

    assert result.exit_code != 0
    assert "--symbols all 不能与其他代码混用" in result.output


def test_cli_update_future_symbols_all_shows_full_universe_hint():
    fake_updater = Mock()
    fake_updater.update_futures_daily = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--asset-class", "future",
                "--dataset", "daily",
                "--symbols", "all",
                "--start-date", "2024-04-01",
                "--end-date", "2024-04-30",
            ],
        )

    assert result.exit_code == 0
    assert "全量合约池" in result.output
    fake_updater.update_futures_daily.assert_awaited_once_with(
        symbols=["all"],
        trade_date=None,
        start_date="2024-04-01",
        end_date="2024-04-30",
        force_update=False,
        progress_callback=ANY,
    )


def test_cli_update_future_minute_trade_date_passed_to_updater():
    fake_updater = Mock()
    fake_updater.update_futures_minute = AsyncMock(return_value=0)
    fake_updater.last_futures_minute_summary = {
        "total_symbols": 0,
        "attempted_symbols": 0,
        "inserted_symbols": 0,
        "empty_symbols": 0,
        "up_to_date_symbols": 0,
        "failed_symbols": [],
        "inserted_records": 0,
    }

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--asset-class", "future",
                "--dataset", "minute_1",
                "--symbols", "all",
                "--trade-date", "2024-04-30",
            ],
        )

    assert result.exit_code == 0
    fake_updater.update_futures_minute.assert_awaited_once_with(
        symbols=["all"],
        trade_date="2024-04-30",
        start_date=None,
        end_date=None,
        freq="1m",
        force_update=False,
        progress_callback=ANY,
    )


def test_cli_update_index_daily_default_uses_full_index_catalog():
    fake_updater = Mock()
    fake_updater.update_index_daily = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "index_daily",
            ],
        )

    assert result.exit_code == 0
    fake_updater.update_index_daily.assert_awaited_once_with(
        ts_code_list=None,
        start_date=None,
        end_date=ANY,
        force_update=False,
        progress_callback=ANY,
    )


def test_cli_update_index_basic_defaults_to_all_tushare_markets():
    fake_updater = Mock()
    fake_updater.update_index_basic = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(app, ["update", "--dataset", "index_basic"])

    assert result.exit_code == 0
    fake_updater.update_index_basic.assert_awaited_once_with(markets=None)


def test_cli_update_index_basic_uses_symbols_as_markets():
    fake_updater = Mock()
    fake_updater.update_index_basic = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            ["update", "--dataset", "index_basic", "--symbols", "sse,sw"],
        )

    assert result.exit_code == 0
    fake_updater.update_index_basic.assert_awaited_once_with(markets=["SSE", "SW"])


def test_cli_update_index_basic_rejects_date_arguments():
    result = runner.invoke(
        app,
        ["update", "--dataset", "index_basic", "--trade-date", "2024-01-02"],
    )

    assert result.exit_code != 0
    assert "index_basic 是非时间序列数据" in result.output


def test_cli_update_index_weight_symbols_all_uses_full_index_catalog():
    fake_updater = Mock()
    fake_updater.resolve_index_weight_codes = AsyncMock(return_value=[
        "000300.CSI",
        "000905.CSI",
    ])
    fake_updater.update_index_weight = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "index_weight",
                "--symbols",
                "all",
                "--start-date",
                "2024-01-01",
                "--end-date",
                "2024-01-31",
            ],
        )

    assert result.exit_code == 0
    fake_updater.resolve_index_weight_codes.assert_awaited_once_with(active_date="2024-01-01")
    fake_updater.update_index_weight.assert_awaited_once_with(
        index_list=["000300.CSI", "000905.CSI"],
        start_date="2024-01-01",
        end_date="2024-01-31",
        trade_date=None,
        force_update=True,
        progress_callback=ANY,
    )


def test_cli_update_index_symbols_all_cannot_mix_with_other_codes():
    result = runner.invoke(
        app,
        [
            "update",
            "--dataset",
            "index_daily",
            "--symbols",
            "all,000300.SH",
        ],
    )

    assert result.exit_code != 0
    assert "--symbols all 不能与其他代码混用" in result.output


def test_cli_update_index_daily_trade_date_uses_batch_updater():
    fake_updater = Mock()
    fake_updater.initialize = AsyncMock()
    fake_updater.close = AsyncMock()
    fake_updater.update_index_daily = AsyncMock(return_value=0)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_updater):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "index_daily",
                "--trade-date",
                "2024-06-30",
            ],
        )

    assert result.exit_code == 0
    fake_updater.update_index_daily.assert_awaited_once_with(
        trade_date="2024-06-30",
        force_update=True,
        progress_callback=ANY,
    )
    fake_updater.close.assert_awaited_once()


def test_cli_update_index_daily_force_all_keeps_catalog_request_for_date_batch():
    fake_updater = Mock()
    fake_updater.update_index_daily = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "index_daily",
                "--symbols",
                "all",
                "--force",
            ],
        )

    assert result.exit_code == 0
    fake_updater.update_index_daily.assert_awaited_once_with(
        ts_code_list=None,
        start_date=None,
        end_date=ANY,
        force_update=True,
        progress_callback=ANY,
    )


def test_cli_update_index_weight_trade_date_uses_local_index_catalog():
    fake_updater = Mock()
    fake_updater.initialize = AsyncMock()
    fake_updater.close = AsyncMock()
    fake_updater.resolve_index_weight_codes = AsyncMock(
        return_value=["000300.CSI", "000905.CSI"]
    )
    fake_updater.update_index_weight = AsyncMock(return_value=0)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_updater):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset",
                "index_weight",
                "--trade-date",
                "2024-06-30",
            ],
        )

    assert result.exit_code == 0
    fake_updater.resolve_index_weight_codes.assert_awaited_once_with(
        active_date="2024-06-30"
    )
    fake_updater.update_index_weight.assert_awaited_once_with(
        index_list=["000300.CSI", "000905.CSI"],
        trade_date="2024-06-30",
        force_update=True,
        progress_callback=ANY,
    )
    fake_updater.close.assert_awaited_once()


def test_cli_update_future_minute_partial_failure_shows_error_sample():
    fake_updater = Mock()
    fake_updater.update_futures_minute = AsyncMock(return_value=0)
    fake_updater.last_futures_minute_summary = {
        "total_symbols": 2,
        "attempted_symbols": 2,
        "inserted_symbols": 0,
        "empty_symbols": 0,
        "up_to_date_symbols": 0,
        "failed_symbols": [
            {
                "symbol": "RB2601.SHF",
                "error": "xtquant_helper temporarily unavailable: 503",
            }
        ],
        "inserted_records": 0,
    }

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--asset-class", "future",
                "--dataset", "minute_1",
                "--symbols", "all",
                "--trade-date", "2024-04-30",
            ],
        )

    assert result.exit_code == 0
    assert "错误样例" in result.output
    assert "xtquant_helper temporarily unavailable: 503" in result.output


def test_cli_update_future_weekly_trade_date_clears_default_end_date():
    fake_updater = Mock()
    fake_updater.update_futures_weekly = AsyncMock(return_value=0)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--asset-class",
                "future",
                "--dataset",
                "weekly",
                "--symbols",
                "all",
                "--trade-date",
                "2024-04-30",
            ],
        )

    assert result.exit_code == 0
    fake_updater.update_futures_weekly.assert_awaited_once_with(
        symbols=["all"],
        trade_date="2024-04-30",
        start_date=None,
        end_date=None,
        force_update=False,
        progress_callback=ANY,
    )


def test_cli_update_future_term_metrics_passes_progress_callback():
    fake_updater = Mock()
    fake_updater.preprocess_futures_term_metrics = AsyncMock(return_value=1)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            [
                "update",
                "--asset-class", "future",
                "--dataset", "term_metrics",
                "--symbols", "all",
                "--force",
            ],
        )

    assert result.exit_code == 0
    fake_updater.preprocess_futures_term_metrics.assert_awaited_once_with(
        product_codes=["all"],
        start_date=None,
        end_date=ANY,
        progress_callback=ANY,
    )


def test_cli_update_daily_basic():
    """测试每日基本面数据更新"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "daily_basic",
            "--symbols", "600519.SH"
        ])
        assert result.exit_code == 0
        assert "数据类型: daily_basic" in result.output


def test_cli_update_hk_basic_does_not_require_existing_symbol_pool():
    """港股 basic 首刷不应依赖数据库中已有股票池。"""

    fake_updater = Mock()
    fake_updater.update_stock_basic = AsyncMock(return_value=2)

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            ["update", "--dataset", "basic", "--market", "HK"],
        )

    assert result.exit_code == 0
    assert "请先执行: fdh-cli update --dataset basic" not in result.output
    fake_updater.update_stock_basic.assert_awaited_once_with(market="HK")


def test_cli_update_hk_daily_trade_date_passes_progress_callback():
    """港股 daily 交易日模式应显示逐股票进度。"""

    fake_updater = Mock()
    fake_updater.initialize = AsyncMock()
    fake_updater.close = AsyncMock()
    fake_updater.update_daily_data = AsyncMock(return_value=2)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_updater):
        result = runner.invoke(
            app,
            [
                "update",
                "--dataset", "daily",
                "--market", "HK",
                "--trade-date", "2024-01-02",
            ],
        )

    assert result.exit_code == 0
    fake_updater.initialize.assert_awaited_once()
    fake_updater.update_daily_data.assert_awaited_once_with(
        trade_date="2024-01-02",
        market="HK",
        force_update=True,
        progress_callback=ANY,
    )
    fake_updater.close.assert_awaited_once()


def test_cli_update_adj_factor():
    """测试复权因子数据更新"""
    with patch('finance_data_hub.update.updater.DataUpdater.update_stock_basic', return_value=0):
        result = runner.invoke(app, [
            "update",
            "--dataset", "adj_factor",
            "--symbols", "600519.SH"
        ])
        assert result.exit_code == 0
        assert "数据类型: adj_factor" in result.output


def test_cli_update_single_symbol_adj_factor_failure_exits_nonzero():
    """单只股票 adj_factor 更新失败时，CLI 应直接报错退出。"""

    fake_updater = Mock()
    fake_updater.update_adj_factor = AsyncMock(side_effect=ValueError("bad adj factor row"))

    fake_context = Mock()
    fake_context.__aenter__ = AsyncMock(return_value=fake_updater)
    fake_context.__aexit__ = AsyncMock(return_value=None)

    with patch("finance_data_hub.cli.main.DataUpdater", return_value=fake_context):
        result = runner.invoke(
            app,
            ["update", "--dataset", "adj_factor", "--symbols", "00700.HK", "--market", "HK"],
        )

    assert result.exit_code != 0
    assert "bad adj factor row" in result.output


def test_cli_preprocess_hk_fundamental_rejected():
    """港股当前不支持基本面预处理，应明确报错。"""
    result = runner.invoke(
        app,
        ["preprocess", "run", "--all", "--category", "fundamental", "--market", "HK"],
    )

    assert result.exit_code != 0
    assert "仅支持技术指标预处理" in result.output


def test_estimate_fetch_start_date_adds_warmup_for_weekly_indicators():
    """技术预处理在带 start_date 回填时，应补足周线指标 warm-up 窗口。"""
    from finance_data_hub.cli.preprocess import _estimate_fetch_start_date

    requested_start = "2019-01-04"
    fetch_start = _estimate_fetch_start_date(
        requested_start,
        freqs=["weekly"],
        indicators=["ma_20", "ma_50", "macd", "rsi_14", "atr_14"],
    )

    assert fetch_start is not None
    assert pd.to_datetime(fetch_start) < pd.to_datetime(requested_start)
    assert (pd.to_datetime(requested_start) - pd.to_datetime(fetch_start)).days >= 490


def test_estimate_records_per_symbol_covers_monthly_ma50_history():
    """月线 MA50 回填至少需要约 50 个月对应的日线历史。"""
    from finance_data_hub.cli.preprocess import _estimate_records_per_symbol

    records = _estimate_records_per_symbol(
        freqs=["monthly"],
        indicators=["ma_20", "ma_50", "macd", "rsi_14", "atr_14"],
    )

    assert records >= 1100


def test_build_incremental_upsert_rule_for_new_daily_data():
    """日线增量应只回写 latest_processed 之后的新交易日。"""
    from finance_data_hub.cli.preprocess import _build_incremental_upsert_rule

    rule = _build_incremental_upsert_rule(
        "daily",
        pd.Timestamp("2026-04-09 15:00:00", tz="Asia/Shanghai"),
        pd.Timestamp("2026-04-08 15:00:00", tz="Asia/Shanghai"),
    )

    assert rule == {"start_date": pd.Timestamp("2026-04-08").date(), "inclusive": False}


def test_build_incremental_upsert_rule_for_open_week():
    """周线若当前周已经存在部分结果，应包含当前周同一周末日期。"""
    from finance_data_hub.cli.preprocess import _build_incremental_upsert_rule

    rule = _build_incremental_upsert_rule(
        "weekly",
        pd.Timestamp("2026-04-09 15:00:00", tz="Asia/Shanghai"),
        pd.Timestamp("2026-04-10 15:00:00", tz="Asia/Shanghai"),
    )

    assert rule == {"start_date": pd.Timestamp("2026-04-10").date(), "inclusive": True}


def test_build_incremental_upsert_rule_for_new_month():
    """月线在进入新月份但尚未生成当月记录时，应从上个已处理月之后回写。"""
    from finance_data_hub.cli.preprocess import _build_incremental_upsert_rule

    rule = _build_incremental_upsert_rule(
        "monthly",
        pd.Timestamp("2026-04-09 15:00:00", tz="Asia/Shanghai"),
        pd.Timestamp("2026-03-31 15:00:00", tz="Asia/Shanghai"),
    )

    assert rule == {"start_date": pd.Timestamp("2026-03-31").date(), "inclusive": False}


def test_fundamental_preprocess_backfill_uses_warmup_and_full_end_day(monkeypatch):
    """基本面日期回填应多取历史计算分位，并写入完整 end_date 当天。"""
    from finance_data_hub.cli import preprocess as preprocess_module

    class FakeResult:
        def __init__(self, rows):
            self._rows = rows

        def fetchall(self):
            return self._rows

    class FakeDbManager:
        def __init__(self):
            self.calls = []

        async def initialize(self):
            return None

        async def execute_raw_sql(self, sql, params=None):
            params = params or {}
            self.calls.append((sql, params))
            if "FROM v_daily_basic_enriched" in sql:
                rows = []
                for day in pd.date_range("2026-05-31", "2026-06-05", freq="D"):
                    rows.append((
                        day + pd.Timedelta(hours=15),
                        "000001.SZ",
                        10.0,
                        1.0,
                        2.0,
                        3.0,
                    ))
                return FakeResult(rows)
            if "FROM fina_indicator" in sql:
                return FakeResult([])
            return FakeResult([])

    captured = {}

    class FakeStorage:
        def __init__(self, db_manager):
            self.db_manager = db_manager

        async def upsert(self, df):
            captured["df"] = df.copy()
            return len(df)

    monkeypatch.setattr(preprocess_module, "FundamentalDataStorage", FakeStorage)
    monkeypatch.setattr(
        preprocess_module.concurrent.futures,
        "ProcessPoolExecutor",
        concurrent.futures.ThreadPoolExecutor,
    )

    db_manager = FakeDbManager()
    result = asyncio.run(
        preprocess_module._run_fundamental_preprocess(
            db_manager=db_manager,
            symbols=["000001.SZ"],
            start_date="2026-06-01",
            end_date="2026-06-05",
            batch_size=1,
            max_concurrent=1,
            num_workers=1,
        )
    )

    daily_sql, daily_params = next(
        call for call in db_manager.calls if "FROM v_daily_basic_enriched" in call[0]
    )
    assert "time < :end_date" in daily_sql
    assert daily_params["end_date"] == pd.Timestamp("2026-06-06").to_pydatetime()
    assert daily_params["start_date"] < pd.Timestamp("2026-06-01").to_pydatetime()
    assert (
        pd.Timestamp("2026-06-01") - pd.Timestamp(daily_params["start_date"])
    ).days >= 1800

    written_dates = pd.to_datetime(captured["df"]["time"]).dt.date.tolist()
    assert written_dates == [
        pd.Timestamp("2026-06-01").date(),
        pd.Timestamp("2026-06-02").date(),
        pd.Timestamp("2026-06-03").date(),
        pd.Timestamp("2026-06-04").date(),
        pd.Timestamp("2026-06-05").date(),
    ]
    assert result == {"symbols_processed": 1, "records_processed": 5}
