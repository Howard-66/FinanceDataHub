"""
CLI 模块单元测试
"""

from unittest.mock import Mock, patch
from typer.testing import CliRunner

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
    """测试更新命令"""
    result = runner.invoke(app, ["update"])
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
