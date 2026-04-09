"""
CLI 模块单元测试
"""

import pytest
from unittest.mock import Mock, patch
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
